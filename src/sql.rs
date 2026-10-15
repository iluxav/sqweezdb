//! SQL front-end: sqlparser AST -> small typed plans -> execution on `table::Database`.
//! Single-table access paths (PK, unique, PK range, scan), two-table equi-joins, aggregates, GROUP BY, ORDER BY + LIMIT,
//! INSERT, UPDATE by PK, BEGIN/COMMIT/ROLLBACK. Prepared statements cached by text.
use crate::table::*;
use sqlparser::ast::{self, BinaryOperator, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, JoinConstraint, JoinOperator, LimitClause, ObjectName, ObjectNamePart, OrderByKind, SelectItem, SetExpr, Statement, TableFactor, TableObject, UnaryOperator};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use crate::util::FastMap;
use std::sync::{Arc, Mutex};

#[derive(Clone)] pub enum Expr { Lit(Value), Param(usize), List(Vec<Expr>), Set(Arc<SelectPlan>), Scalar(Arc<SelectPlan>) }
impl std::fmt::Debug for Expr { fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { match self { Expr::Lit(v) => write!(f, "{v:?}"), Expr::Param(i) => write!(f, "?{i}"), Expr::List(l) => write!(f, "list({})", l.len()), Expr::Set(_) => write!(f, "(subquery)"), Expr::Scalar(_) => write!(f, "(scalar subquery)") } } }
#[derive(Clone, Copy, Debug, PartialEq)] pub enum Cmp { Eq, Ne, Lt, Le, Gt, Ge, IsNull, IsNotNull, In, NotIn }
#[derive(Clone, Debug)] pub struct Pred { pub col: usize, pub cmp: Cmp, pub rhs: Expr }
#[derive(Clone, Debug)] pub enum Agg { CountStar, CountCol(usize), Sum(usize), Avg(usize), Min(usize), Max(usize) }
#[derive(Clone, Debug)] pub enum Proj { Col(usize), Agg(Agg), Sub(usize) }
#[derive(Clone)] pub enum Access { Pk(Expr), Unique(Expr), Range { lo: Expr, lo_excl: bool, hi: Expr, hi_excl: bool }, Index(Arc<SecIndex>, Expr), Scan }
/// Equi-join chain. The probe table is scanned (or reached by key/index); each step joins one more table whose join
/// column is its primary key, copying that table's needed columns next to the row, so the pipeline sees one virtual row.
pub struct JoinStep { pub build: Arc<Table>, pub build_key: usize, pub key_vidx: usize, pub build_preds: Vec<Pred>, pub build_cols: Vec<usize>, pub base: usize, pub vbase: usize, pub build_access: Access }
pub struct JoinPlan { pub probe: Arc<Table>, pub probe_access: Access, pub probe_preds: Vec<Pred>, pub steps: Vec<JoinStep>, pub index_first: Option<Arc<SecIndex>> }
pub struct SelectPlan { pub table: Arc<Table>, pub schema: Schema, pub access: Access, pub preds: Vec<Pred>, pub proj: Vec<Proj>, pub group_by: Option<usize>, pub order: Option<(usize, bool)>, pub limit: Option<usize>, pub join: Option<JoinPlan>, pub corr: Option<(usize, usize)>, pub subs: Vec<(Arc<SelectPlan>, Option<usize>)> }
pub enum Plan { CreateTable(Schema), Insert { table: Arc<Table>, exprs: Vec<Expr> }, Select(SelectPlan), Update { table: Arc<Table>, sets: Vec<(usize, Expr)>, pk: Expr }, Delete { table: Arc<Table>, access: Access, preds: Vec<Pred> }, Begin, Commit, Rollback, Alter { table: String, ops: Vec<AlterOp> } }
#[derive(Clone, Debug)] pub enum AlterOp { Add { col: Column, default: Value }, Drop(String), Rename(String, String), SetType(String, ColType), SetNotNull(String), DropNotNull(String) }
pub struct Prepared { pub plan: Plan, pub nparams: usize, pub text: String, pub epoch: u64 }
#[derive(Default)] pub struct Session { pub txn: Option<Txn>, pub user: Option<String>, pub roles: Vec<String>, pub attrs: HashMap<String, Value> }

// ---------- access control: roles, users, table grants, row policies ----------
pub const P_SELECT: u8 = 1; pub const P_INSERT: u8 = 2; pub const P_UPDATE: u8 = 4; pub const P_DELETE: u8 = 8;
#[derive(Clone, Debug)] pub enum PolicyRhs { Lit(Value), Attr(String) }
#[derive(Clone, Debug)] pub struct Policy { pub name: String, pub table: String, pub role: String, pub cmds: u8, pub preds: Vec<(String, Cmp, PolicyRhs)> }
#[derive(Clone, Debug, Default)] pub struct UserRec { pub roles: Vec<String>, pub attrs: HashMap<String, Value> }
#[derive(Default)] pub struct Acl { pub roles: std::collections::HashSet<String>, pub users: HashMap<String, UserRec>, pub grants: HashMap<(String, String), u8>, pub policies: Vec<Policy> }

fn tokens(s: &str) -> Vec<String> {
    let cs: Vec<char> = s.chars().collect(); let mut out = vec![]; let mut i = 0;
    while i < cs.len() {
        let c = cs[i];
        if c.is_whitespace() { i += 1; continue; }
        if c == '\'' { let mut j = i + 1; while j < cs.len() && cs[j] != '\'' { j += 1; } out.push(cs[i..=j.min(cs.len() - 1)].iter().collect()); i = j + 1; continue; }
        if c.is_alphanumeric() || c == '_' { let mut j = i; while j < cs.len() && (cs[j].is_alphanumeric() || cs[j] == '_' || cs[j] == '.') { j += 1; } out.push(cs[i..j].iter().collect()); i = j; continue; }
        if (c == '<' || c == '>') && i + 1 < cs.len() && (cs[i + 1] == '=' || cs[i + 1] == '>') { out.push(cs[i..i + 2].iter().collect()); i += 2; continue; }
        if c == ';' { i += 1; continue; }
        out.push(c.to_string()); i += 1;
    }
    out
}
fn literal_tok(t: &str) -> Result<Value, String> {
    if t.starts_with('\'') { return Ok(Value::Text(t.trim_matches('\'').to_string())); }
    if let Ok(i) = t.parse::<i64>() { return Ok(Value::Int(i)); }
    if let Ok(f) = t.parse::<f64>() { return Ok(Value::Float(f)); }
    match t.to_uppercase().as_str() { "TRUE" => Ok(Value::Bool(true)), "FALSE" => Ok(Value::Bool(false)), _ => Err(format!("bad literal {t}")) }
}
fn priv_of(t: &str) -> Result<u8, String> { match t.to_uppercase().as_str() { "SELECT" => Ok(P_SELECT), "INSERT" => Ok(P_INSERT), "UPDATE" => Ok(P_UPDATE), "DELETE" => Ok(P_DELETE), "ALL" => Ok(P_SELECT | P_INSERT | P_UPDATE | P_DELETE), x => Err(format!("unknown privilege {x}")) } }
fn is_admin_stmt(sql: &str) -> bool { let u = sql.trim_start().to_uppercase(); u.starts_with("CREATE ROLE ") || u.starts_with("CREATE USER ") || u.starts_with("GRANT ") || u.starts_with("CREATE POLICY ") || u.starts_with("CREATE INDEX ") }

fn obj_name(n: &ObjectName) -> String { n.0.iter().map(|p| match p { ObjectNamePart::Identifier(i) => i.value.clone(), other => format!("{other}") }).collect::<Vec<_>>().join(".") }
fn default_value(e: &ast::Expr) -> Result<Value, String> {
    match e {
        ast::Expr::Value(v) => match &v.value { ast::Value::Number(s, _) => Ok(if let Ok(i) = s.parse::<i64>() { Value::Int(i) } else { Value::Float(s.parse::<f64>().map_err(|e| e.to_string())?) }), ast::Value::SingleQuotedString(s) => Ok(Value::Text(s.clone())), ast::Value::Boolean(b) => Ok(Value::Bool(*b)), ast::Value::Null => Ok(Value::Null), other => Err(format!("unsupported default {other}")) },
        ast::Expr::UnaryOp { op: UnaryOperator::Minus, expr } => match default_value(expr)? { Value::Int(i) => Ok(Value::Int(-i)), Value::Float(f) => Ok(Value::Float(-f)), v => Err(format!("bad default {v:?}")) },
        ast::Expr::Identifier(i) => literal_tok(&i.value),
        other => Err(format!("unsupported default {other}")),
    }
}
fn coltype_of(dt: &ast::DataType) -> Result<ColType, String> {
    let s = format!("{dt}").to_uppercase();
    let num = |s: &str| s.split(|c| c == '(' || c == ')').nth(1).and_then(|x| x.trim().parse::<usize>().ok());
    Ok(match s.as_str() {
        "INT" | "INTEGER" | "BIGINT" | "INT64" | "SMALLINT" => ColType::Int,
        "DOUBLE" | "DOUBLE PRECISION" | "REAL" | "FLOAT" | "FLOAT64" => ColType::Float,
        "BOOL" | "BOOLEAN" => ColType::Bool,
        "TEXT" => ColType::Text(255), "BLOB" | "BYTEA" => ColType::Bytes(255),
        _ if s.starts_with("VARCHAR") || s.starts_with("CHAR") || s.starts_with("TEXT") || s.starts_with("STRING") => ColType::Text(num(&s).ok_or(format!("need a length: {s}"))?),
        _ if s.starts_with("BLOB") || s.starts_with("VARBINARY") || s.starts_with("BYTES") || s.starts_with("BINARY") => ColType::Bytes(num(&s).ok_or(format!("need a length: {s}"))?),
        _ => return Err(format!("unsupported type {s}")),
    })
}
fn conjuncts<'e>(e: &'e ast::Expr, out: &mut Vec<&'e ast::Expr>) {
    match e { ast::Expr::BinaryOp { left, op: BinaryOperator::And, right } => { conjuncts(left, out); conjuncts(right, out); } ast::Expr::Nested(x) => conjuncts(x, out), other => out.push(other) }
}

#[derive(Clone)] struct Src { table: Arc<Table>, alias: String }
enum Ref { Inner(usize, usize), Outer(usize, usize), None }
/// Resolve a column reference against the FROM sources: Ok(Some((side, col))), Ok(None) if not an identifier.
fn jcol(srcs: &[Src], e: &ast::Expr) -> Result<Option<(usize, usize)>, String> {
    match e {
        ast::Expr::Identifier(i) => {
            let m: Vec<(usize, usize)> = srcs.iter().enumerate().filter_map(|(s, src)| src.table.schema.col(&i.value).map(|c| (s, c))).collect();
            match m.len() { 0 => Err(format!("unknown column {}", i.value)), 1 => Ok(Some(m[0])), _ => Err(format!("ambiguous column {}", i.value)) }
        }
        ast::Expr::CompoundIdentifier(v) if v.len() == 2 => {
            let s = srcs.iter().position(|src| src.alias.eq_ignore_ascii_case(&v[0].value)).ok_or(format!("unknown table or alias {}", v[0].value))?;
            Ok(Some((s, srcs[s].table.schema.col(&v[1].value).ok_or(format!("unknown column {}.{}", v[0].value, v[1].value))?)))
        }
        ast::Expr::Nested(x) => jcol(srcs, x),
        _ => Ok(None),
    }
}

struct Planner<'a> { db: &'a Database, nparams: usize, outer: Vec<Src>, cur: Vec<Src>, corr: Option<(usize, usize, usize, usize)>, exists_mode: bool }
impl<'a> Planner<'a> {
    fn resolve_ref(&self, srcs: &[Src], e: &ast::Expr) -> Result<Ref, String> {
        match jcol(srcs, e) { Ok(Some((s, c))) => Ok(Ref::Inner(s, c)), Ok(None) => Ok(Ref::None), Err(err) => { if !self.outer.is_empty() { if let Ok(Some((s, c))) = jcol(&self.outer, e) { return Ok(Ref::Outer(s, c)); } } Err(err) } }
    }
    /// Plan a subquery with the given outer scope; `exists` ignores its projection and selects the correlated column.
    fn plan_sub(&mut self, q: &ast::Query, outer: &[Src], exists: bool) -> Result<SelectPlan, String> {
        let saved = (std::mem::replace(&mut self.outer, outer.to_vec()), self.corr.take(), std::mem::replace(&mut self.exists_mode, exists));
        let r = self.plan_query(q);
        self.outer = saved.0; self.corr = saved.1; self.exists_mode = saved.2;
        match r? { Plan::Select(sp) => Ok(sp), _ => Err("bad subquery".into()) }
    }
    fn expr(&mut self, e: &ast::Expr) -> Result<Expr, String> {
        match e {
            ast::Expr::Value(v) => match &v.value {
                ast::Value::Number(s, _) => Ok(Expr::Lit(if let Ok(i) = s.parse::<i64>() { Value::Int(i) } else { Value::Float(s.parse::<f64>().map_err(|e| e.to_string())?) })),
                ast::Value::SingleQuotedString(s) => Ok(Expr::Lit(Value::Text(s.clone()))),
                ast::Value::Boolean(b) => Ok(Expr::Lit(Value::Bool(*b))),
                ast::Value::Null => Ok(Expr::Lit(Value::Null)),
                ast::Value::Placeholder(_) => { let i = self.nparams; self.nparams += 1; Ok(Expr::Param(i)) }
                other => Err(format!("unsupported literal {other}")),
            },
            ast::Expr::UnaryOp { op: UnaryOperator::Minus, expr } => match self.expr(expr)? { Expr::Lit(Value::Int(i)) => Ok(Expr::Lit(Value::Int(-i))), Expr::Lit(Value::Float(f)) => Ok(Expr::Lit(Value::Float(-f))), _ => Err("cannot negate".into()) },
            ast::Expr::Nested(e) => self.expr(e),
            ast::Expr::Subquery(q) => { let cur = self.cur.clone(); let sp = self.plan_sub(q, &cur, false)?; if sp.corr.is_none() && sp.proj.len() != 1 { return Err("scalar subquery must select one column".into()); } Ok(Expr::Scalar(Arc::new(sp))) }
            other => Err(format!("unsupported expression {other}")),
        }
    }
    fn table_src(&self, tf: &TableFactor) -> Result<Src, String> {
        match tf { TableFactor::Table { name, alias, .. } => { let n = obj_name(name); let table = self.db.table(&n).ok_or(format!("no such table {n}"))?; Ok(Src { table, alias: alias.as_ref().map(|a| a.name.value.clone()).unwrap_or(n) }) }, other => Err(format!("unsupported FROM {other}")) }
    }
    fn plan(&mut self, st: &Statement) -> Result<Plan, String> {
        match st {
            Statement::CreateTable(ct) => {
                let mut cols = vec![]; let mut pk = None; let mut unique = None; let mut fks: Vec<FkDef> = vec![];
                let action = |a: &Option<ast::ReferentialAction>| -> Result<FkAction, String> { match a { None | Some(ast::ReferentialAction::NoAction) | Some(ast::ReferentialAction::Restrict) => Ok(FkAction::Restrict), Some(ast::ReferentialAction::Cascade) => Ok(FkAction::Cascade), Some(ast::ReferentialAction::SetNull) => Ok(FkAction::SetNull), Some(other) => Err(format!("unsupported referential action {other}")) } };
                for (i, c) in ct.columns.iter().enumerate() {
                    let mut nullable = true;
                    let mut default = Value::Null;
                    for o in &c.options { let s = format!("{}", o.option).to_uppercase(); if let ast::ColumnOption::ForeignKey(fk) = &o.option { fks.push(FkDef { col: i, parent: obj_name(&fk.foreign_table), on_delete: action(&fk.on_delete)? }); } else if s.starts_with("PRIMARY KEY") { pk = Some(i); nullable = false; } else if s.starts_with("UNIQUE") { unique = Some(i); } else if s.starts_with("NOT NULL") { nullable = false; } else if let ast::ColumnOption::Default(e) = &o.option { default = default_value(e)?; } }
                    cols.push(Column { name: c.name.value.clone(), ty: coltype_of(&c.data_type)?, nullable, default });
                }
                for tc in &ct.constraints { if let ast::TableConstraint::ForeignKey(fk) = tc { if fk.columns.len() != 1 { return Err("one column per foreign key (for now)".into()); } let col = cols.iter().position(|c| c.name.eq_ignore_ascii_case(&fk.columns[0].value)).ok_or(format!("unknown column {}", fk.columns[0].value))?; fks.push(FkDef { col, parent: obj_name(&fk.foreign_table), on_delete: action(&fk.on_delete)? }); } }
                let mut schema = Schema::new(&obj_name(&ct.name), cols, pk.ok_or("a PRIMARY KEY column is required")?, unique)?; schema.fks = fks;
                Ok(Plan::CreateTable(schema))
            }
            Statement::Insert(ins) => {
                let name = match &ins.table { TableObject::TableName(n) => obj_name(n), other => return Err(format!("unsupported insert target {other}")) };
                let table = self.db.table(&name).ok_or(format!("no such table {name}"))?;
                let q = ins.source.as_ref().ok_or("INSERT needs VALUES")?;
                let row: &Vec<ast::Expr> = match &*q.body { SetExpr::Values(v) => &v.rows.first().ok_or("empty VALUES")?.content, _ => return Err("only INSERT ... VALUES is supported".into()) };
                let mut exprs: Vec<Option<Expr>> = vec![None; table.schema.columns.len()];
                if ins.columns.is_empty() { if row.len() != exprs.len() { return Err(format!("expected {} values", exprs.len())); } for (i, e) in row.iter().enumerate() { exprs[i] = Some(self.expr(e)?); } }
                else { for (c, e) in ins.columns.iter().zip(row) { let n = obj_name(c); let i = table.schema.col(&n).ok_or(format!("no such column {n}"))?; exprs[i] = Some(self.expr(e)?); } }
                let exprs = exprs.into_iter().enumerate().map(|(i, e)| { let c = &table.schema.columns[i]; match e { Some(e) => Ok(e), None if c.default != Value::Null => Ok(Expr::Lit(c.default.clone())), None if c.nullable => Ok(Expr::Lit(Value::Null)), None => Err(format!("column {} has no value and no default", c.name)) } }).collect::<Result<Vec<_>, _>>()?;
                Ok(Plan::Insert { table, exprs })
            }
            Statement::Query(q) => self.plan_query(q),
            Statement::Delete(d) => self.plan_delete(d),
            Statement::Update(u) => {
                let src = self.table_src(&u.table.relation)?; let table = src.table.clone();
                let srcs = vec![src]; let schema = table.schema.clone();
                let mut sets = vec![];
                for a in &u.assignments { let n = match &a.target { ast::AssignmentTarget::ColumnName(o) => obj_name(o), other => return Err(format!("unsupported assignment {other}")) }; let c = schema.col(&n).ok_or(format!("no such column {n}"))?; let e = self.expr(&a.value)?; sets.push((c, e)); }
                let mut preds = vec![vec![]];
                if let Some(w) = &u.selection { let mut cj = vec![]; conjuncts(w, &mut cj); for c in cj { self.classify(&srcs, c, &mut preds, &mut vec![])?; } }
                if preds[0].len() != 1 || preds[0][0].col != schema.pk || preds[0][0].cmp != Cmp::Eq { return Err("UPDATE needs WHERE <primary key> = <value> (for now)".into()); }
                Ok(Plan::Update { table, sets, pk: preds[0].pop().unwrap().rhs })
            }
            Statement::AlterTable(at) => {
                let mut ops = vec![];
                for op in &at.operations {
                    ops.push(match op {
                        ast::AlterTableOperation::AddColumn { column_def: c, .. } => { let mut nullable = true; let mut default = Value::Null; for o in &c.options { match &o.option { ast::ColumnOption::NotNull => nullable = false, ast::ColumnOption::Default(e) => default = default_value(e)?, ast::ColumnOption::Null => nullable = true, other => return Err(format!("unsupported column option {other}")) } } if !nullable && default == Value::Null { return Err(format!("column {} is NOT NULL and needs a DEFAULT for existing rows", c.name.value)); } AlterOp::Add { col: Column { name: c.name.value.clone(), ty: coltype_of(&c.data_type)?, nullable, default: default.clone() }, default } }
                        ast::AlterTableOperation::DropColumn { column_names, .. } => { if column_names.len() != 1 { return Err("drop one column at a time".into()); } AlterOp::Drop(column_names[0].value.clone()) }
                        ast::AlterTableOperation::RenameColumn { old_column_name, new_column_name } => AlterOp::Rename(old_column_name.value.clone(), new_column_name.value.clone()),
                        ast::AlterTableOperation::AlterColumn { column_name, op } => match op { ast::AlterColumnOperation::SetNotNull => AlterOp::SetNotNull(column_name.value.clone()), ast::AlterColumnOperation::DropNotNull => AlterOp::DropNotNull(column_name.value.clone()), ast::AlterColumnOperation::SetDataType { data_type, .. } => AlterOp::SetType(column_name.value.clone(), coltype_of(data_type)?), other => return Err(format!("unsupported ALTER COLUMN {other}")) },
                        other => return Err(format!("unsupported ALTER TABLE operation {other}")),
                    });
                }
                Ok(Plan::Alter { table: obj_name(&at.name), ops })
            }
            Statement::StartTransaction { .. } => Ok(Plan::Begin),
            Statement::Commit { .. } => Ok(Plan::Commit),
            Statement::Rollback { .. } => Ok(Plan::Rollback),
            other => Err(format!("unsupported statement {other}")),
        }
    }
    /// Put one WHERE/ON conjunct into a side's predicate list, or record it as the join condition.
    fn classify(&mut self, srcs: &[Src], e: &ast::Expr, preds: &mut Vec<Vec<Pred>>, join: &mut Vec<(usize, usize, usize, usize)>) -> Result<(), String> {
        match e {
            ast::Expr::BinaryOp { left, op, right } => {
                let cmp = match op { BinaryOperator::Eq => Cmp::Eq, BinaryOperator::NotEq => Cmp::Ne, BinaryOperator::Lt => Cmp::Lt, BinaryOperator::LtEq => Cmp::Le, BinaryOperator::Gt => Cmp::Gt, BinaryOperator::GtEq => Cmp::Ge, o => return Err(format!("unsupported operator {o}")) };
                match (self.resolve_ref(srcs, left)?, self.resolve_ref(srcs, right)?) {
                    (Ref::Inner(sa, ca), Ref::Inner(sb, cb)) => { if sa == sb || cmp != Cmp::Eq { return Err(format!("unsupported predicate {e}")); } join.push((sa, ca, sb, cb)); Ok(()) }
                    (Ref::Inner(s, c), Ref::Outer(os, oc)) | (Ref::Outer(os, oc), Ref::Inner(s, c)) => {
                        if cmp != Cmp::Eq { return Err("a correlated predicate must be an equality".into()); }
                        if os != 0 { return Err("a correlated reference must point at the outer query's first table".into()); }
                        if self.corr.is_some() { return Err("one correlated equality per subquery".into()); }
                        self.corr = Some((s, c, os, oc)); Ok(())
                    }
                    (Ref::Inner(s, c), Ref::None) => { let rhs = self.expr(right)?; preds[s].push(Pred { col: c, cmp, rhs }); Ok(()) }
                    (Ref::None, Ref::Inner(s, c)) => { let rhs = self.expr(left)?; let flipped = match cmp { Cmp::Lt => Cmp::Gt, Cmp::Le => Cmp::Ge, Cmp::Gt => Cmp::Lt, Cmp::Ge => Cmp::Le, x => x }; preds[s].push(Pred { col: c, cmp: flipped, rhs }); Ok(()) }
                    (Ref::Outer(..), _) | (_, Ref::Outer(..)) => Err("outer columns may only appear in the correlated equality".into()),
                    _ => Err(format!("unsupported predicate {e}")),
                }
            }
            ast::Expr::Between { expr, negated: false, low, high } => { let (s, c) = jcol(srcs, expr)?.ok_or(format!("unknown column in {e}"))?; let lo = self.expr(low)?; let hi = self.expr(high)?; preds[s].push(Pred { col: c, cmp: Cmp::Ge, rhs: lo }); preds[s].push(Pred { col: c, cmp: Cmp::Le, rhs: hi }); Ok(()) }
            ast::Expr::InSubquery { expr, subquery, negated } => {
                let (s, c) = jcol(srcs, expr)?.ok_or(format!("unknown column in {e}"))?;
                let sp = self.plan_sub(subquery, srcs, false)?;
                if sp.corr.is_none() && sp.proj.len() != 1 { return Err("IN (subquery) must select exactly one column".into()); }
                preds[s].push(Pred { col: c, cmp: if *negated { Cmp::NotIn } else { Cmp::In }, rhs: Expr::Set(Arc::new(sp)) }); Ok(())
            }
            ast::Expr::Exists { subquery, negated } => {
                let sp = self.plan_sub(subquery, srcs, true)?;
                let (_, oc) = sp.corr.ok_or("EXISTS needs a correlation with the outer query (inner.col = outer.col)")?;
                preds[0].push(Pred { col: oc, cmp: if *negated { Cmp::NotIn } else { Cmp::In }, rhs: Expr::Set(Arc::new(sp)) }); Ok(())
            }
            ast::Expr::InList { expr, list, negated } => {
                let (s, c) = jcol(srcs, expr)?.ok_or(format!("unknown column in {e}"))?;
                let items = list.iter().map(|x| self.expr(x)).collect::<Result<Vec<_>, _>>()?;
                preds[s].push(Pred { col: c, cmp: if *negated { Cmp::NotIn } else { Cmp::In }, rhs: Expr::List(items) }); Ok(())
            }
            ast::Expr::IsNull(x) => { let (s, c) = jcol(srcs, x)?.ok_or(format!("unknown column in {e}"))?; preds[s].push(Pred { col: c, cmp: Cmp::IsNull, rhs: Expr::Lit(Value::Null) }); Ok(()) }
            ast::Expr::IsNotNull(x) => { let (s, c) = jcol(srcs, x)?.ok_or(format!("unknown column in {e}"))?; preds[s].push(Pred { col: c, cmp: Cmp::IsNotNull, rhs: Expr::Lit(Value::Null) }); Ok(()) }
            ast::Expr::Nested(x) => self.classify(srcs, x, preds, join),
            other => Err(format!("unsupported predicate {other}")),
        }
    }
    fn plan_query(&mut self, q: &ast::Query) -> Result<Plan, String> {
        let sel = match &*q.body { SetExpr::Select(s) => s, _ => return Err("unsupported query shape".into()) };
        // sources
        let mut srcs = vec![]; let mut on_exprs: Vec<&ast::Expr> = vec![];
        for twj in &sel.from {
            srcs.push(self.table_src(&twj.relation)?);
            for j in &twj.joins {
                match &j.join_operator { JoinOperator::Inner(JoinConstraint::On(e)) | JoinOperator::Join(JoinConstraint::On(e)) => conjuncts(e, &mut on_exprs), other => return Err(format!("only INNER JOIN ... ON is supported, got {other:?}")) }
                srcs.push(self.table_src(&j.relation)?);
            }
        }
        if srcs.is_empty() || srcs.len() > 8 { return Err("between one and eight tables in FROM".into()); }
        let exists = std::mem::replace(&mut self.exists_mode, false);
        let saved_cur = std::mem::replace(&mut self.cur, srcs.clone());
        // predicates
        let mut side_preds: Vec<Vec<Pred>> = vec![vec![]; srcs.len()];
        let mut conds: Vec<(usize, usize, usize, usize)> = vec![];
        let mut cj = on_exprs; if let Some(w) = &sel.selection { conjuncts(w, &mut cj); }
        for c in cj { self.classify(&srcs, c, &mut side_preds, &mut conds)?; }
        let corr_spec = self.corr.take();
        let mut subs: Vec<(Arc<SelectPlan>, Option<usize>)> = vec![];
        let joined = srcs.len() > 1;
        // join order: a probe table plus a chain of steps, each joining a table on ITS primary key through a column
        // already in the row. Try each table as the probe, prefer the largest one.
        let mut order: Option<(usize, Vec<(usize, usize, usize, usize)>)> = None;   // probe, steps (build side, build col, key side, key col)
        if joined {
            let mut cands: Vec<usize> = (0..srcs.len()).collect();
            cands.sort_by_key(|&i| std::cmp::Reverse(srcs[i].table.rows_allocated()));
            'outer: for &p in &cands {
                let mut inc = vec![p]; let mut steps = vec![]; let mut used = vec![false; conds.len()];
                loop {
                    let mut progressed = false;
                    for (ci, &(sa, ca, sb, cb)) in conds.iter().enumerate() {
                        if used[ci] { continue; }
                        let (ia, ib) = (inc.contains(&sa), inc.contains(&sb));
                        if ia && !ib && cb == srcs[sb].table.schema.pk { steps.push((sb, cb, sa, ca)); inc.push(sb); used[ci] = true; progressed = true; }
                        else if ib && !ia && ca == srcs[sa].table.schema.pk { steps.push((sa, ca, sb, cb)); inc.push(sa); used[ci] = true; progressed = true; }
                    }
                    if !progressed { break; }
                }
                if inc.len() == srcs.len() { if used.iter().any(|u| !u) { return Err("a join condition is neither a primary-key join nor supported as a filter".into()); } order = Some((p, steps)); break 'outer; }
            }
            if order.is_none() { return Err("cannot order the joins: every joined table must be reached through its primary key".into()); }
        }
        let (probe, step_specs) = order.unwrap_or((0, vec![]));
        let ps = srcs[probe].table.schema.clone();
        let ncols_probe = ps.columns.len();
        // virtual column allocation: probe columns first, then each step's needed build columns in step order
        let mut step_cols: Vec<Vec<usize>> = vec![vec![]; step_specs.len()];
        let side_to_step: HashMap<usize, usize> = step_specs.iter().enumerate().map(|(k, sp)| (sp.0, k)).collect();
        let mut total = ncols_probe; // running count, recomputed later
        let _ = &mut total;
        let mut vidx = |side: usize, col: usize| -> usize {
            if !joined || side == probe { return col; }
            let k = side_to_step[&side];
            if let Some(p) = step_cols[k].iter().position(|&c| c == col) { ncols_probe + step_cols[..k].iter().map(|v| v.len()).sum::<usize>() + p }
            else { step_cols[k].push(col); ncols_probe + step_cols[..k].iter().map(|v| v.len()).sum::<usize>() + step_cols[k].len() - 1 }
        };
        // step keys must be in the virtual row: register them first (a key may come from the probe or an earlier step)
        let mut key_vidx = vec![0usize; step_specs.len()];
        for (k, &(_, _, ks, kc)) in step_specs.iter().enumerate() { key_vidx[k] = vidx(ks, kc); }
        let mut proj = vec![];
        for item in sel.projection.iter().filter(|_| !exists) {
            match item {
                SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                    if let ast::Expr::Subquery(q) = e { let srcs_c = srcs.clone(); let sp = self.plan_sub(q, &srcs_c, false)?; let oc = sp.corr.map(|(_, oc)| oc); subs.push((Arc::new(sp), oc)); proj.push(Proj::Sub(subs.len() - 1)); continue; }
                    if let Some((s, c)) = jcol(&srcs, e)? { proj.push(Proj::Col(vidx(s, c))); continue; }
                    if let ast::Expr::Function(f) = e {
                        let name = obj_name(&f.name).to_lowercase();
                        let arg = match &f.args { FunctionArguments::List(l) => l.args.first(), _ => None };
                        let argcol = |a: Option<&FunctionArg>| -> Result<(usize, usize), String> { match a { Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(e))) => jcol(&srcs, e)?.ok_or(format!("{name} needs a column")), _ => Err(format!("{name} needs a column")) } };
                        let agg = match name.as_str() { "count" => match arg { Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(e))) => { let (s, c) = jcol(&srcs, e)?.ok_or("count needs a column or *")?; Agg::CountCol(vidx(s, c)) }, _ => Agg::CountStar }, "sum" => { let (s, c) = argcol(arg)?; Agg::Sum(vidx(s, c)) }, "avg" => { let (s, c) = argcol(arg)?; Agg::Avg(vidx(s, c)) }, "min" => { let (s, c) = argcol(arg)?; Agg::Min(vidx(s, c)) }, "max" => { let (s, c) = argcol(arg)?; Agg::Max(vidx(s, c)) }, _ => return Err(format!("unsupported function {name}")) };
                        proj.push(Proj::Agg(agg)); continue;
                    }
                    return Err(format!("unsupported projection {e}"));
                }
                SelectItem::Wildcard(_) => { for i in 0..ncols_probe { proj.push(Proj::Col(i)); } for &(bs, _, _, _) in &step_specs { for c in 0..srcs[bs].table.schema.columns.len() { proj.push(Proj::Col(vidx(bs, c))); } } }
                other => return Err(format!("unsupported projection {other}")),
            }
        }
        let group_by = match &sel.group_by { GroupByExpr::Expressions(v, _) if !v.is_empty() => { if v.len() > 1 { return Err("one GROUP BY column only".into()); } let (s, c) = jcol(&srcs, &v[0])?.ok_or("unknown GROUP BY column")?; Some(vidx(s, c)) }, _ => None };
        // no "bare" columns: with an aggregate or a GROUP BY, every plain column must be the grouping column (SQLite picks an arbitrary row instead; E29)
        if group_by.is_some() || proj.iter().any(|p| matches!(p, Proj::Agg(_))) { for p in &proj { if let Proj::Col(v) = p { if Some(*v) != group_by { return Err("every selected column must appear in GROUP BY or inside an aggregate".into()); } } } }
        let mut order_by = None;
        if let Some(ob) = &q.order_by { if let OrderByKind::Expressions(v) = &ob.kind { if let Some(o) = v.first() { let (s, c) = jcol(&srcs, &o.expr)?.ok_or("ORDER BY must name a column")?; let vi = vidx(s, c); let idx = proj.iter().position(|p| matches!(p, Proj::Col(x) if *x == vi)).ok_or("ORDER BY column must be selected")?; order_by = Some((idx, format!("{}", o.options).to_uppercase().contains("DESC"))); } } }
        let limit = match &q.limit_clause { Some(LimitClause::LimitOffset { limit: Some(e), .. }) => match self.expr(e)? { Expr::Lit(Value::Int(n)) => Some(n as usize), _ => return Err("LIMIT must be a number".into()) }, _ => None };
        // a correlated subquery's output is shaped for its use: EXISTS -> the correlated column; one aggregate -> grouped by
        // the correlated column; one column -> that column plus the correlated column (key pairs)
        let mut group_by = group_by;
        let corr = match corr_spec { Some((s, c, _, oc)) => Some((vidx(s, c), oc)), None => None };
        if let Some((cv, _)) = corr {
            if exists { proj = vec![Proj::Col(cv)]; group_by = None; }
            else if proj.len() == 1 { match proj[0] { Proj::Agg(_) => { let a = proj.remove(0); proj = vec![Proj::Col(cv), a]; group_by = Some(cv); } Proj::Col(_) => proj.push(Proj::Col(cv)), _ => return Err("unsupported correlated subquery shape".into()) } }
            else { return Err("a correlated subquery must select one column or one aggregate".into()); }
        } else if exists { return Err("EXISTS needs a correlation with the outer query".into()); }
        drop(vidx);
        // virtual schema: [null bitmap over all virtual columns][probe row][step 0 cols][step 1 cols]...
        let mut schema = ps.clone();
        let mut steps = vec![];
        if joined {
            let ncols_total = ncols_probe + step_cols.iter().map(|v| v.len()).sum::<usize>();
            let vn = (ncols_total + 7) / 8;
            schema.null_bytes = vn; for o in schema.offsets.iter_mut() { *o += vn; }
            let mut off = vn + ps.row_size; let mut vbase = ncols_probe;
            for (k, &(bs, bcol, _, _)) in step_specs.iter().enumerate() {
                let bsch = &srcs[bs].table.schema; let base = off;
                for &c in &step_cols[k] { schema.columns.push(Column { name: format!("{}.{}", srcs[bs].alias, bsch.columns[c].name), ty: bsch.columns[c].ty.clone(), nullable: bsch.columns[c].nullable, default: Value::Null }); schema.offsets.push(off); off += bsch.columns[c].ty.width(); }
                let mut build_preds = std::mem::take(&mut side_preds[bs]);
                let build_access = access_path(&srcs[bs].table, &mut build_preds);
                steps.push(JoinStep { build: srcs[bs].table.clone(), build_key: bcol, key_vidx: key_vidx[k], build_preds, build_cols: step_cols[k].clone(), base, vbase, build_access });
                vbase += step_cols[k].len();
            }
            schema.row_size = off; schema.unique = None; schema.name = srcs.iter().map(|s| s.alias.clone()).collect::<Vec<_>>().join("*");
            let mut probe_preds = std::mem::take(&mut side_preds[probe]);
            let probe_access = access_path(&srcs[probe].table, &mut probe_preds);
            // index nested loop when the first step's table is selective and the probe key is indexed
            let index_first = None; // decided adaptively at execution from the filtered build sizes
            let join = JoinPlan { probe: srcs[probe].table.clone(), probe_access, probe_preds, steps, index_first };
            self.cur = saved_cur;
            return Ok(Plan::Select(SelectPlan { table: srcs[probe].table.clone(), schema, access: Access::Scan, preds: vec![], proj, group_by, order: order_by, limit, join: Some(join), corr, subs }));
        }
        let mut preds = std::mem::take(&mut side_preds[0]);
        let access = access_path(&srcs[0].table, &mut preds);
        self.cur = saved_cur;
        Ok(Plan::Select(SelectPlan { table: srcs[0].table.clone(), schema, access, preds, proj, group_by, order: order_by, limit, join: None, corr, subs }))
    }
    fn plan_delete(&mut self, d: &ast::Delete) -> Result<Plan, String> {
        let twj = match &d.from { ast::FromTable::WithFromKeyword(v) | ast::FromTable::WithoutKeyword(v) => v.first().ok_or("DELETE needs FROM")? };
        let src = self.table_src(&twj.relation)?; let table = src.table.clone(); let srcs = vec![src];
        let mut preds = vec![vec![]];
        if let Some(w) = &d.selection { let mut cj = vec![]; conjuncts(w, &mut cj); for c in cj { self.classify(&srcs, c, &mut preds, &mut vec![])?; } }
        let mut preds = preds.pop().unwrap();
        let access = access_path(&table, &mut preds);
        Ok(Plan::Delete { table, access, preds })
    }
}
/// Pick the single-table access path and remove the predicates it absorbs.
fn access_path(table: &Arc<Table>, preds: &mut Vec<Pred>) -> Access {
    let schema = &table.schema;
    if let Some(i) = preds.iter().position(|p| p.col == schema.pk && p.cmp == Cmp::Eq) { return Access::Pk(preds.remove(i).rhs); }
    if let Some(i) = schema.unique.and_then(|u| preds.iter().position(|p| p.col == u && p.cmp == Cmp::Eq)) { return Access::Unique(preds.remove(i).rhs); }
    if let Some((i, ix)) = preds.iter().enumerate().find_map(|(i, p)| if p.cmp == Cmp::Eq { table.index_on(p.col).map(|ix| (i, ix)) } else { None }) { return Access::Index(ix, preds.remove(i).rhs); }
    let lo = preds.iter().position(|p| p.col == schema.pk && matches!(p.cmp, Cmp::Ge | Cmp::Gt));
    let hi = preds.iter().position(|p| p.col == schema.pk && matches!(p.cmp, Cmp::Le | Cmp::Lt));
    if let (Some(a), Some(b)) = (lo, hi) {
        let (pa, pb) = (preds[a].clone(), preds[b].clone());
        let mut rm = vec![a, b]; rm.sort_unstable(); for i in rm.into_iter().rev() { preds.remove(i); }
        return Access::Range { lo: pa.rhs, lo_excl: pa.cmp == Cmp::Gt, hi: pb.rhs, hi_excl: pb.cmp == Cmp::Lt };
    }
    Access::Scan
}

// ---------- execution ----------

#[derive(Clone, Copy, Default)] struct AggState { n: u64, sum_f: f64, sum_i: i64, min: f64, max: f64, seen: bool }
/// A set of integer keys: bitmap when the range is reasonable, hash map otherwise.
pub enum KeySet { Bits { min: i64, max: i64, bits: Vec<u64> }, Hash(FastMap<()>), Empty }
impl KeySet {
    pub fn build(keys: &[i64]) -> KeySet {
        if keys.is_empty() { return KeySet::Empty; }
        let (min, max) = keys.iter().fold((i64::MAX, i64::MIN), |(a, b), &k| (a.min(k), b.max(k)));
        if (max - min) as u64 + 1 <= 128_000_000 { let mut bits = vec![0u64; ((max - min) as usize >> 6) + 1]; for &k in keys { let d = (k - min) as usize; bits[d >> 6] |= 1 << (d & 63); } KeySet::Bits { min, max, bits } }
        else { let mut m = FastMap::default(); m.reserve(keys.len()); for &k in keys { m.insert(k, ()); } KeySet::Hash(m) }
    }
    #[inline] pub fn contains(&self, k: i64) -> bool { match self { KeySet::Bits { min, max, bits } => k >= *min && k <= *max && { let d = (k - min) as usize; bits[d >> 6] >> (d & 63) & 1 == 1 }, KeySet::Hash(m) => m.contains_key(&k), KeySet::Empty => false } }
}
#[inline] fn mix_pair(a: i64, b: i64) -> i64 { ((a as u64).wrapping_mul(0x9E3779B97F4A7C15) ^ (b as u64).rotate_left(32).wrapping_mul(0xC2B2AE3D27D4EB4F)) as i64 }
pub struct CPred { off: usize, kind: u8, cmp: Cmp, i: i64, f: f64, b: Vec<u8>, nb: usize, nm: u8, set: Option<Arc<KeySet>>, pair: Option<(usize, usize, u8)>, corr_map: Option<Arc<FastMap<f64>>>, ckind: u8 }
impl CPred {
    #[inline] fn ok<T: PartialOrd>(cmp: Cmp, a: T, b: T) -> bool { match cmp { Cmp::Eq => a == b, Cmp::Ne => a != b, Cmp::Lt => a < b, Cmp::Le => a <= b, Cmp::Gt => a > b, Cmp::Ge => a >= b, _ => false } }
    #[inline(always)] pub fn eval(&self, row: &[u8]) -> bool {
        let null = row[self.nb] & self.nm != 0;
        match self.cmp {
            Cmp::IsNull => return null, Cmp::IsNotNull => return !null,
            Cmp::In | Cmp::NotIn => {
                if null { return false; }
                let mut k = read_i64(row, self.off);
                if let Some((po, pnb, pnm)) = self.pair { if row[pnb] & pnm != 0 { return false; } k = mix_pair(k, read_i64(row, po)); }
                let hit = self.set.as_ref().unwrap().contains(k);
                return if self.cmp == Cmp::In { hit } else { !hit };
            }
            _ => if null { return false; }
        }
        if self.kind == 20 {
            // correlated scalar: compare this column with the inner aggregate for this row's key
            let (co, cnb, cnm) = self.pair.unwrap();
            if row[cnb] & cnm != 0 { return false; }
            let v = match self.corr_map.as_ref().unwrap().get(&read_i64(row, co)) { Some(v) => *v, None => return false };
            let a = if self.ckind == 1 { read_f64(row, self.off) } else { read_i64(row, self.off) as f64 };
            return Self::ok(self.cmp, a, v);
        }
        match self.kind { 0 => Self::ok(self.cmp, read_i64(row, self.off), self.i), 1 => Self::ok(self.cmp, read_f64(row, self.off), self.f), 2 => Self::ok(self.cmp, row[self.off] as i64, self.i), 9 => false, _ => Self::ok(self.cmp, read_bytes(row, self.off), &self.b[..]) }
    }
}

/// Consumes rows that passed the predicates; produces projected rows, aggregates, groups, or top-k.
const DENSE_GROUPS: usize = 65_536;
const LAZY_CAP: usize = 200_000;
enum SubVal<'a> { Const(Value), Map { map: FastMap<Value>, off: usize, nb: usize, nm: u8, dflt: Value }, Lazy { f: Box<dyn Fn(i64) -> Value + 'a>, full: Box<dyn Fn() -> FastMap<Value> + 'a>, dflt: Value, off: usize, nb: usize, nm: u8, cache: std::cell::RefCell<FastMap<Value>>, fallback: std::cell::RefCell<Option<FastMap<Value>>> } }
impl<'a> SubVal<'a> {
    fn get(&self, row: &[u8]) -> Value {
        match self {
            SubVal::Const(v) => v.clone(),
            SubVal::Map { map, off, nb, nm, dflt } => if row[*nb] & nm != 0 { Value::Null } else { map.get(&read_i64(row, *off)).cloned().unwrap_or_else(|| dflt.clone()) },
            SubVal::Lazy { f, full, dflt, off, nb, nm, cache, fallback } => {
                if row[*nb] & nm != 0 { return Value::Null; }
                let k = read_i64(row, *off);
                if let Some(m) = fallback.borrow().as_ref() { return m.get(&k).cloned().unwrap_or_else(|| dflt.clone()); }
                if let Some(v) = cache.borrow().get(&k) { return v.clone(); }
                if cache.borrow().len() >= LAZY_CAP { let m = full(); let v = m.get(&k).cloned().unwrap_or_else(|| dflt.clone()); *fallback.borrow_mut() = Some(m); return v; }  // too many distinct keys: one grouped pass instead
                let v = f(k); cache.borrow_mut().insert(k, v.clone()); v
            }
        }
    }
}
struct Sink<'a> { schema: &'a Schema, proj: &'a [Proj], subs: Vec<SubVal<'a>>, aggs: Vec<&'a Agg>, has_agg: bool, group_by: Option<usize>, group_off: Option<usize>, order: Option<(usize, bool)>, limit: usize, single: Vec<AggState>, groups: FastMap<u32>, gstates: Vec<AggState>, dense: Vec<AggState>, dense_seen: Vec<bool>, null_group: Option<Vec<AggState>>, topk: Vec<(f64, Vec<Value>)>, out: Vec<Vec<Value>> }
impl<'a> Sink<'a> {
    fn new(schema: &'a Schema, proj: &'a [Proj], group_by: Option<usize>, order: Option<(usize, bool)>, limit: Option<usize>, subs: Vec<SubVal<'a>>) -> Sink<'a> {
        let aggs: Vec<&Agg> = proj.iter().filter_map(|p| if let Proj::Agg(a) = p { Some(a) } else { None }).collect();
        Sink { schema, proj, subs, has_agg: !aggs.is_empty(), single: vec![AggState::default(); aggs.len()], aggs, group_by, group_off: group_by.map(|g| schema.offsets[g]), order, limit: limit.unwrap_or(usize::MAX), groups: FastMap::default(), gstates: vec![], dense: vec![], dense_seen: vec![], null_group: None, topk: vec![], out: vec![] }
    }
    #[inline] fn push(&mut self, row: &[u8]) -> bool {
        if self.has_agg {
            let n = self.aggs.len();
            let st: &mut [AggState] = if let Some(go) = self.group_off {
                if is_null(row, self.group_by.unwrap()) { self.null_group.get_or_insert_with(|| vec![AggState::default(); n]) }
                else {
                    let key = read_i64(row, go);
                    if key >= 0 && (key as usize) < DENSE_GROUPS {
                        // small non-negative keys: direct array instead of a hash map
                        if self.dense.is_empty() { self.dense = vec![AggState::default(); DENSE_GROUPS * n]; self.dense_seen = vec![false; DENSE_GROUPS]; }
                        let k = key as usize; self.dense_seen[k] = true; &mut self.dense[k * n..(k + 1) * n]
                    } else { let gl = self.gstates.len(); let i = *self.groups.entry(key).or_insert_with(|| { gl as u32 }) as usize; if i == gl { self.gstates.extend(std::iter::repeat(AggState::default()).take(n)); } &mut self.gstates[i * n..(i + 1) * n] }
                }
            } else { &mut self.single };
            for (k, a) in self.aggs.iter().enumerate() {
                let st = &mut st[k];
                match a { Agg::CountStar => st.n += 1, Agg::CountCol(c) => if !is_null(row, *c) { st.n += 1 }, Agg::Sum(c) | Agg::Avg(c) | Agg::Min(c) | Agg::Max(c) => {
                    if is_null(row, *c) { continue; }
                    let off = self.schema.offsets[*c];
                    let (fi, ff) = match self.schema.columns[*c].ty { ColType::Int => { let i = read_i64(row, off); (i, i as f64) }, ColType::Float => { let f = read_f64(row, off); (0, f) }, _ => (0, 0.0) };
                    st.n += 1; st.sum_i += fi; st.sum_f += ff; if !st.seen || ff < st.min { st.min = ff; } if !st.seen || ff > st.max { st.max = ff; } st.seen = true; } }
            }
            return true;
        }
        if let Some((oi, desc)) = self.order {
            // compare on raw bytes first; decode only survivors
            let c = match self.proj[oi] { Proj::Col(c) => c, _ => 0 };
            let off = self.schema.offsets[c];
            let key = match self.schema.columns[c].ty { ColType::Int => read_i64(row, off) as f64, ColType::Float => read_f64(row, off), _ => 0.0 };
            let key = if desc { -key } else { key };
            if self.topk.len() >= self.limit && key >= self.topk[self.limit - 1].0 { return true; }
            let vals: Vec<Value> = self.proj.iter().map(|p| match p { Proj::Col(c) => decode_col(self.schema, row, *c), Proj::Agg(_) => Value::Null, Proj::Sub(i) => self.subs[*i].get(row) }).collect();
            let pos = self.topk.partition_point(|x| x.0 <= key); self.topk.insert(pos, (key, vals));
            if self.topk.len() > self.limit { self.topk.pop(); }
            return true;
        }
        let vals: Vec<Value> = self.proj.iter().map(|p| match p { Proj::Col(c) => decode_col(self.schema, row, *c), Proj::Agg(_) => Value::Null, Proj::Sub(i) => self.subs[*i].get(row) }).collect();
        self.out.push(vals);
        self.out.len() < self.limit
    }
    /// Only count(*) aggregates, so a row count can be pushed without the rows.
    fn count_only(&self) -> bool { self.has_agg && self.aggs.iter().all(|a| matches!(a, Agg::CountStar)) }
    fn group_at_or_after(&self, min_col: usize) -> bool { self.group_by.map(|g| g >= min_col).unwrap_or(true) }
    #[inline] fn push_count(&mut self, row: &[u8], n: u64) -> bool {
        let na = self.aggs.len();
        let st: &mut [AggState] = if let Some(go) = self.group_off {
            if is_null(row, self.group_by.unwrap()) { self.null_group.get_or_insert_with(|| vec![AggState::default(); na]) }
            else { let key = read_i64(row, go); if key >= 0 && (key as usize) < DENSE_GROUPS { if self.dense.is_empty() { self.dense = vec![AggState::default(); DENSE_GROUPS * na]; self.dense_seen = vec![false; DENSE_GROUPS]; } let k = key as usize; self.dense_seen[k] = true; &mut self.dense[k * na..(k + 1) * na] } else { let gl = self.gstates.len(); let i = *self.groups.entry(key).or_insert_with(|| gl as u32) as usize; if i == gl { self.gstates.extend(std::iter::repeat(AggState::default()).take(na)); } &mut self.gstates[i * na..(i + 1) * na] } }
        } else { &mut self.single };
        for x in st.iter_mut() { x.n += n; }
        true
    }
    fn finish(mut self) -> Vec<Vec<Value>> {
        if self.has_agg {
            let schema = self.schema; let proj = self.proj; let group_by = self.group_by;
            let emit = |key: Option<i64>, st: &[AggState]| -> Vec<Value> {
                let mut k = 0;
                proj.iter().map(|p| match p {
                    Proj::Col(c) => if Some(*c) == group_by { key.map(Value::Int).unwrap_or(Value::Null) } else { Value::Null },
                    Proj::Sub(_) => Value::Null,
                    Proj::Agg(a) => { let x = st[k]; k += 1; match a {
                        Agg::CountStar | Agg::CountCol(_) => Value::Int(x.n as i64),
                        Agg::Sum(c) => if x.n == 0 { Value::Null } else if schema.columns[*c].ty == ColType::Int { Value::Int(x.sum_i) } else { Value::Float(x.sum_f) },
                        Agg::Avg(_) => if x.n == 0 { Value::Null } else { Value::Float(x.sum_f / x.n as f64) },
                        Agg::Min(c) => if x.n == 0 { Value::Null } else if schema.columns[*c].ty == ColType::Int { Value::Int(x.min as i64) } else { Value::Float(x.min) },
                        Agg::Max(c) => if x.n == 0 { Value::Null } else if schema.columns[*c].ty == ColType::Int { Value::Int(x.max as i64) } else { Value::Float(x.max) } } }
                }).collect()
            };
            if self.group_off.is_some() {
                if let Some(ng) = &self.null_group { let r = emit(None, ng); self.out.push(r); }
                let n = self.aggs.len();
                let mut keys: Vec<i64> = self.groups.keys().copied().collect();
                for (k, seen) in self.dense_seen.iter().enumerate() { if *seen { keys.push(k as i64); } }
                keys.sort_unstable();
                for k in keys { let r = if k >= 0 && (k as usize) < DENSE_GROUPS && !self.dense.is_empty() && self.dense_seen[k as usize] { emit(Some(k), &self.dense[k as usize * n..(k as usize + 1) * n]) } else { let i = self.groups[&k] as usize; emit(Some(k), &self.gstates[i * n..(i + 1) * n]) }; self.out.push(r); }
            }
            else { let r = emit(None, &self.single); self.out.push(r); }
            if self.limit != usize::MAX { self.out.truncate(self.limit); }
            return self.out;
        }
        if self.order.is_some() { return self.topk.into_iter().map(|(_, v)| v).collect(); }
        self.out
    }
}

pub struct Sql { pub db: Arc<Database>, cache: Arc<Mutex<HashMap<String, Arc<Prepared>>>>, pub acl: std::sync::RwLock<Acl>, pub epoch: Arc<std::sync::atomic::AtomicU64>, alters: Mutex<Vec<std::thread::JoinHandle<Result<(), String>>>> }
/// One committed change, decoded against the table's schema.
#[derive(Debug, Clone, PartialEq)] pub enum ChangeOp { Insert, Update, Delete }
#[derive(Debug, Clone)] pub struct Change { pub seq: u64, pub table: String, pub op: ChangeOp, pub pk: i64, pub row: Option<Vec<Value>> }
/// A change feed: tails the log and decodes records into `Change`s. Records written to a table that no longer owns its
/// name (a shadow during a schema change, a retired table after one) are skipped, so one logical change is one event.
pub struct Feed { cursor: LogCursor, db: Arc<Database>, last: u64, on_ring: bool }
impl Drop for Feed { fn drop(&mut self) { self.db.log.feeds.fetch_sub(1, std::sync::atomic::Ordering::Relaxed); } }
impl Feed {
    pub fn last_seq(&self) -> u64 { self.last }
    fn decode(&self, seq: u64, tid: u16, op: u8, payload: &[u8], out: &mut Vec<Change>) {
        let Some(t) = self.db.table_by_id(tid) else { return };
        let (since, until) = (t.owner_since.load(std::sync::atomic::Ordering::Relaxed), t.owner_until.load(std::sync::atomic::Ordering::Relaxed));
        if seq < since || seq >= until { return; }  // written to a shadow before it took the name, or to a retired table after
        match op {
            OP_DELETE => { let pk = if payload.len() == 8 { i64::from_le_bytes(payload.try_into().unwrap()) } else { t.pk_of(payload) }; out.push(Change { seq, table: t.schema.name.clone(), op: ChangeOp::Delete, pk, row: None }); }
            OP_INSERT | OP_UPDATE => { if payload.len() != t.schema.row_size { return; } let row: Vec<Value> = (0..t.schema.columns.len()).map(|i| decode_col(&t.schema, payload, i)).collect(); out.push(Change { seq, table: t.schema.name.clone(), op: if op == OP_INSERT { ChangeOp::Insert } else { ChangeOp::Update }, pk: t.pk_of(payload), row: Some(row) }); }
            _ => {}
        }
    }
    /// New changes since the last poll (empty if none yet). In-process: served from the log's memory ring the moment a
    /// record is committed (durable mode: once it is durable); the files are read only to catch up after a gap.
    pub fn poll(&mut self) -> Vec<Change> {
        let durable_only = self.db.durable;
        let (ring, ok) = self.db.log.recent_after(self.last, durable_only);
        let recs: Vec<(u64, u8, u16, Vec<u8>)> = if ok { self.on_ring = true; ring.into_iter().map(|(a, b, c, p)| (a, b, c, p.to_vec())).collect() }
            else { if self.on_ring { self.cursor = LogCursor::new(&self.db.dir, self.last); self.on_ring = false; } let upto = if durable_only { self.db.log.st.lock().unwrap().durable } else { u64::MAX }; self.cursor.poll_upto(upto) };
        let mut out = vec![];
        for (seq, op, tid, payload) in recs {
            self.last = self.last.max(seq);
            match op {
                OP_TXN => { let mut p = 0; while p + 7 <= payload.len() { let t2 = u16::from_le_bytes(payload[p..p + 2].try_into().unwrap()); let op2 = payload[p + 2]; let len = u32::from_le_bytes(payload[p + 3..p + 7].try_into().unwrap()) as usize; p += 7; self.decode(seq, t2, op2, &payload[p..p + len], &mut out); p += len; } }
                OP_META => {}
                _ => self.decode(seq, tid, op, &payload, &mut out),
            }
        }
        out
    }
}
/// Handle on a follower's replication thread.
pub struct Follower { pub applied: Arc<std::sync::atomic::AtomicU64>, stop: Arc<std::sync::atomic::AtomicBool>, thread: Option<std::thread::JoinHandle<()>> }
impl Follower { pub fn applied(&self) -> u64 { self.applied.load(std::sync::atomic::Ordering::Acquire) } }
impl Drop for Follower { fn drop(&mut self) { self.stop.store(true, std::sync::atomic::Ordering::Relaxed); if let Some(h) = self.thread.take() { h.join().ok(); } } }
impl Sql {
    pub fn new(db: Arc<Database>) -> Sql { Sql { db, cache: Arc::new(Mutex::new(HashMap::new())), acl: std::sync::RwLock::new(Acl::default()), epoch: Arc::new(std::sync::atomic::AtomicU64::new(0)), alters: Mutex::new(vec![]) } }
    /// Build the new schema and the old-row -> new-row converter for a list of ALTER operations.
    fn alter_schema(old: &Schema, ops: &[AlterOp]) -> Result<(Schema, Arc<dyn Fn(&[u8]) -> Result<Vec<u8>, String> + Send + Sync>), String> {
        // working list: (column, source: Some(old index) | None, default, retyped from old type)
        let mut cols: Vec<(Column, Option<usize>, Value)> = old.columns.iter().enumerate().map(|(i, c)| (c.clone(), Some(i), Value::Null)).collect();
        let find = |cols: &Vec<(Column, Option<usize>, Value)>, n: &str| cols.iter().position(|(c, _, _)| c.name.eq_ignore_ascii_case(n)).ok_or(format!("no such column {n}"));
        let pk_name = old.columns[old.pk].name.clone(); let uniq_name = old.unique.map(|u| old.columns[u].name.clone());
        for op in ops {
            match op {
                AlterOp::Add { col, default } => { if find(&cols, &col.name).is_ok() { return Err(format!("column {} already exists", col.name)); } cols.push((col.clone(), None, default.clone())); }
                AlterOp::Drop(n) => { let i = find(&cols, n)?; if cols[i].0.name.eq_ignore_ascii_case(&pk_name) { return Err("cannot drop the primary key".into()); } cols.remove(i); }
                AlterOp::Rename(a, b) => { let i = find(&cols, a)?; if find(&cols, b).is_ok() { return Err(format!("column {b} already exists")); } cols[i].0.name = b.clone(); }
                AlterOp::SetType(n, ty) => { let i = find(&cols, n)?; if cols[i].0.name.eq_ignore_ascii_case(&pk_name) { return Err("cannot change the primary key type".into()); } cols[i].0.ty = ty.clone(); }
                AlterOp::SetNotNull(n) => { let i = find(&cols, n)?; cols[i].0.nullable = false; }
                AlterOp::DropNotNull(n) => { let i = find(&cols, n)?; cols[i].0.nullable = true; }
            }
        }
        let pk = cols.iter().position(|(c, _, _)| c.name.eq_ignore_ascii_case(&pk_name)).ok_or("primary key lost")?;
        let unique = uniq_name.and_then(|u| cols.iter().position(|(c, _, _)| c.name.eq_ignore_ascii_case(&u)));
        let mut schema = Schema::new(&old.name, cols.iter().map(|(c, _, _)| c.clone()).collect(), pk, unique)?;
        schema.fks = old.fks.iter().filter_map(|f| cols.iter().position(|(_, src, _)| *src == Some(f.col)).map(|col| FkDef { col, parent: f.parent.clone(), on_delete: f.on_delete.clone() })).collect();
        let (old_s, new_s) = (old.clone(), schema.clone()); let srcs: Vec<(Option<usize>, Value)> = cols.iter().map(|(_, s, d)| (*s, d.clone())).collect();
        let convert = Arc::new(move |row: &[u8]| -> Result<Vec<u8>, String> {
            let vals: Vec<Value> = srcs.iter().enumerate().map(|(i, (src, dflt))| match src { Some(j) => { let v = decode_col(&old_s, row, *j); if v == Value::Null && !new_s.columns[i].nullable { Value::Null } else { v } } None => dflt.clone() }).collect();
            encode_row(&new_s, &vals).map_err(|e| format!("row with key {} cannot be converted: {e}", read_i64(row, old_s.offsets[old_s.pk])))
        });
        Ok((schema, convert))
    }
    /// Online ALTER TABLE: shadow table, mirrored writes, background copy, swap. Returns as soon as the shadow exists;
    /// `wait_alters` blocks until the copy and the swap are done.
    fn alter(&self, text: &str, table: &str, ops: &[AlterOp], log: bool) -> Result<(), String> {
        let old = self.db.table(table).ok_or(format!("no such table {table}"))?;
        let (schema, convert) = Self::alter_schema(&old.schema, ops)?;
        self.db.alter_start(table, schema, convert)?;
        if log { self.log_meta(text); }
        self.spawn_copy(table.to_string());
        Ok(())
    }
    fn spawn_copy(&self, table: String) {
        let (db, cache, epoch) = (self.db.clone(), self.cache.clone(), self.epoch.clone());
        let h = std::thread::spawn(move || {
            let meta = |text: &str| { let seq = db.log.append_meta(text); if db.durable { db.log.wait_durable(seq); } };
            let old = db.table(&table).ok_or("table vanished")?;
            let sh = old.shadow.read().unwrap().clone().ok_or("no shadow")?;
            let stop = std::sync::atomic::AtomicBool::new(false);
            match old.copy_into_shadow(&sh, &stop) {
                Ok(_) => { let old = db.alter_finish(&table)?; meta(&format!("ALTER TABLE {table} DONE")); cache.lock().unwrap().clear(); epoch.fetch_add(1, std::sync::atomic::Ordering::Release); std::thread::sleep(std::time::Duration::from_secs(1)); old.clear(); Ok(()) }  // grace period: statements planned before the swap drain on the old table, still mirrored
                Err(e) => { db.alter_abort(&table)?; meta(&format!("ALTER TABLE {table} ABORT")); Err(e) }
            }
        });
        self.alters.lock().unwrap().push(h);
    }
    /// Wait for every background schema change; returns the first error, if any.
    pub fn wait_alters(&self) -> Result<(), String> { let hs: Vec<_> = self.alters.lock().unwrap().drain(..).collect(); let mut r = Ok(()); for h in hs { if let Err(e) = h.join().map_err(|_| "alter thread panicked".to_string())? { r = Err(e); } } r }
    /// Create a fresh database directory and start it.
    pub fn create(dir: &std::path::Path, durable: bool) -> Sql { let _ = std::fs::remove_dir_all(dir); let db = Arc::new(Database::open(dir, durable)); db.start(); Sql::new(db) }
    /// Open an existing database: replay the snapshot catalog + rows, then the log tail, then start the committer.
    pub fn open(dir: &std::path::Path, durable: bool, threads: usize) -> Result<(Sql, RecStats), String> {
        let db = Arc::new(Database::open(dir, durable));
        let sql = Sql::new(db.clone());
        let st = db.recover(threads, &mut |text| sql.apply_meta(text))?;
        db.start();
        Ok((sql, st))
    }
    /// Like `open`, and resumes any schema change that was interrupted (its shadow exists, no DONE/ABORT logged).
    pub fn open_resume(dir: &std::path::Path, durable: bool, threads: usize) -> Result<(Sql, RecStats, usize), String> {
        let (sql, st) = Sql::open(dir, durable, threads)?;
        let pending = sql.db.tables_with_shadow(); let n = pending.len();
        for (name, _) in pending { sql.spawn_copy(name); }
        Ok((sql, st, n))
    }
    pub fn snapshot(&self) -> SnapStats { self.db.snapshot() }
    /// Open as a follower of another database directory: map its snapshot, replay its log, then keep applying new
    /// records as the owner commits them. Never writes to that directory and never starts a committer. Reads are
    /// ordinary SQL; `applied` is the last sequence seen.
    pub fn follow(dir: &std::path::Path, poll: std::time::Duration) -> Result<(Arc<Sql>, Follower), String> {
        let db = Arc::new(Database::open(dir, false));
        let sql = Arc::new(Sql::new(db.clone()));
        let st = db.recover(1, &mut |text| sql.apply_meta(text))?;
        let applied = Arc::new(std::sync::atomic::AtomicU64::new(db.log.st.lock().unwrap().seq)); let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (db2, applied2, stop2, dir2, sql2) = (db.clone(), applied.clone(), stop.clone(), dir.to_path_buf(), sql.clone());
        let h = std::thread::spawn(move || {
            let mut cur = LogCursor::new(&dir2, applied2.load(std::sync::atomic::Ordering::Relaxed));
            while !stop2.load(std::sync::atomic::Ordering::Relaxed) {
                let recs = cur.poll();
                if recs.is_empty() { std::thread::sleep(poll); continue; }
                for (seq, op, table, payload) in recs { if let Err(e) = db2.apply_record(seq, op, table, &payload, &mut |t| sql2.apply_meta(t)) { eprintln!("follower: {e}"); } applied2.store(seq, std::sync::atomic::Ordering::Release); }
            }
        });
        let _ = st;
        Ok((sql, Follower { applied, stop, thread: Some(h) }))
    }
    /// Subscribe to committed changes after `after_seq` (0 = everything still in the log). Resumable: keep `Feed::last_seq`.
    pub fn subscribe(&self, after_seq: u64) -> Feed { self.db.log.feeds.fetch_add(1, std::sync::atomic::Ordering::Relaxed); Feed { cursor: LogCursor::new(&self.db.dir, after_seq), db: self.db.clone(), last: after_seq, on_ring: false } }
    pub fn current_seq(&self) -> u64 { self.db.log.st.lock().unwrap().seq }
    /// Copy the snapshot and the log segments to another directory (a consistent backup as long as no snapshot runs meanwhile).
    pub fn backup(&self, dest: &std::path::Path) -> Result<u64, String> {
        std::fs::create_dir_all(dest).map_err(|e| e.to_string())?; let mut bytes = 0;
        for e in std::fs::read_dir(&self.db.dir).map_err(|e| e.to_string())?.flatten() { let n = e.file_name(); let n = n.to_string_lossy(); if n == "snap" || n.starts_with("log.") { bytes += std::fs::copy(e.path(), dest.join(&*n)).map_err(|e| e.to_string())?; } }
        Ok(bytes)
    }
    /// Restore a backup directory to the state just after log sequence `upto`, writing a fresh snapshot there.
    pub fn restore(src: &std::path::Path, dest: &std::path::Path, upto: u64, threads: usize) -> Result<(Sql, RecStats), String> {
        let _ = std::fs::remove_dir_all(dest); std::fs::create_dir_all(dest).map_err(|e| e.to_string())?;
        for e in std::fs::read_dir(src).map_err(|e| e.to_string())?.flatten() { let n = e.file_name(); let n = n.to_string_lossy(); if n == "snap" || n.starts_with("log.") { std::fs::copy(e.path(), dest.join(&*n)).map_err(|e| e.to_string())?; } }
        let db = Arc::new(Database::open(dest, false));
        let sql = Sql::new(db.clone());
        let st = db.recover_upto(threads, &mut |text| sql.apply_meta(text), upto)?;
        // drop the log tail beyond `upto`: a fresh snapshot makes the restored state self-contained
        db.start(); sql.snapshot();
        Ok((sql, st))
    }
    /// Execute a catalog statement (DDL or ACL) without logging it.
    fn apply_meta(&self, text: &str) -> Result<(), String> {
        let app = Session::default();
        if is_admin_stmt(text) { return self.admin(&app, text); }
        let u = text.trim().to_uppercase();
        if u.starts_with("ALTER TABLE ") && (u.ends_with(" DONE") || u.ends_with(" ABORT")) { let name = text.trim().split_whitespace().nth(2).unwrap_or(""); return if u.ends_with(" DONE") { self.db.alter_finish(name).map(|old| old.clear()) } else { self.db.alter_abort(name) }; }
        let stmts = Parser::parse_sql(&GenericDialect {}, text).map_err(|e| e.to_string())?;
        let mut pl = Planner { db: &self.db, nparams: 0, outer: vec![], cur: vec![], corr: None, exists_mode: false };
        match pl.plan(&stmts[0])? {
            Plan::CreateTable(s) => { self.db.create_table(s)?; Ok(()) }
            Plan::Alter { table, ops } => { let old = self.db.table(&table).ok_or(format!("no such table {table}"))?; let (schema, convert) = Self::alter_schema(&old.schema, &ops)?; self.db.alter_start(&table, schema, convert)?; Ok(()) }  // the copy resumes in open_arc
            _ => Err(format!("not a catalog statement: {text}")) }
    }
    fn log_meta(&self, text: &str) { let seq = self.db.log.append_meta(text); if self.db.durable { self.db.log.wait_durable(seq); } }
    /// Bind a session to a named user (roles and attributes are copied in). A session without a user is the embedding
    /// application itself and is unrestricted.
    pub fn login(&self, sess: &mut Session, user: &str) -> Result<(), String> {
        let acl = self.acl.read().unwrap();
        let u = acl.users.get(user).ok_or(format!("no such user {user}"))?;
        sess.user = Some(user.to_string()); sess.roles = u.roles.clone(); sess.attrs = u.attrs.clone(); Ok(())
    }
    fn admin(&self, sess: &Session, sql: &str) -> Result<(), String> {
        if sess.user.is_some() { return Err("permission denied: only the application session can manage roles, users and policies".into()); }
        let t = tokens(sql); let up: Vec<String> = t.iter().map(|x| x.to_uppercase()).collect();
        let mut acl = self.acl.write().unwrap();
        let at = |i: usize| -> Result<&String, String> { t.get(i).ok_or(format!("unexpected end of statement: {sql}")) };
        match (up[0].as_str(), up.get(1).map(|x| x.as_str())) {
            ("CREATE", Some("INDEX")) => {
                // CREATE INDEX name ON table ( col )
                let name = at(2)?.clone(); if up.get(3).map(|x| x.as_str()) != Some("ON") { return Err("expected ON".into()); }
                let table = at(4)?.to_lowercase(); let tbl = self.db.table(&table).ok_or(format!("no such table {table}"))?;
                if at(5)? != "(" || at(7)? != ")" { return Err("expected ( column )".into()); }
                let col = tbl.schema.col(at(6)?).ok_or(format!("no such column {}", at(6)?))?;
                tbl.create_index(&name, col)?; self.cache.lock().unwrap().clear(); Ok(())
            }
            ("CREATE", Some("ROLE")) => { acl.roles.insert(at(2)?.clone()); Ok(()) }
            ("CREATE", Some("USER")) => {
                let name = at(2)?.clone(); let mut rec = UserRec::default(); let mut i = 3;
                while i < t.len() {
                    if up[i] == "IN" && up.get(i + 1).map(|x| x.as_str()) == Some("ROLE") { let r = at(i + 2)?.clone(); if !acl.roles.contains(&r) { return Err(format!("no such role {r}")); } rec.roles.push(r); i += 3; }
                    else if up[i] == "WITH" { i += 1; loop { let k = at(i)?.clone(); if at(i + 1)? != "=" { return Err("expected = in WITH".into()); } rec.attrs.insert(k, literal_tok(at(i + 2)?)?); i += 3; if t.get(i).map(|x| x.as_str()) == Some(",") { i += 1; continue; } break; } }
                    else { return Err(format!("unexpected token {}", t[i])); }
                }
                acl.users.insert(name, rec); Ok(())
            }
            ("GRANT", _) => {
                let mut i = 1; let mut mask = 0u8;
                loop { mask |= priv_of(at(i)?)?; i += 1; if t.get(i).map(|x| x.as_str()) == Some(",") { i += 1; continue; } break; }
                if up.get(i).map(|x| x.as_str()) != Some("ON") { return Err("expected ON".into()); }
                let table = at(i + 1)?.to_lowercase(); if self.db.table(&table).is_none() { return Err(format!("no such table {table}")); }
                if up.get(i + 2).map(|x| x.as_str()) != Some("TO") { return Err("expected TO".into()); }
                let role = at(i + 3)?.clone(); if !acl.roles.contains(&role) { return Err(format!("no such role {role}")); }
                *acl.grants.entry((role, table)).or_insert(0) |= mask; Ok(())
            }
            ("CREATE", Some("POLICY")) => {
                // CREATE POLICY name ON table FOR cmd TO role USING ( col op rhs [AND ...] )
                let name = at(2)?.clone();
                if up.get(3).map(|x| x.as_str()) != Some("ON") { return Err("expected ON".into()); }
                let table = at(4)?.to_lowercase(); let tbl = self.db.table(&table).ok_or(format!("no such table {table}"))?;
                if up.get(5).map(|x| x.as_str()) != Some("FOR") { return Err("expected FOR".into()); }
                let cmds = priv_of(at(6)?)?;
                if up.get(7).map(|x| x.as_str()) != Some("TO") { return Err("expected TO".into()); }
                let role = at(8)?.clone(); if !acl.roles.contains(&role) { return Err(format!("no such role {role}")); }
                if up.get(9).map(|x| x.as_str()) != Some("USING") || at(10)? != "(" { return Err("expected USING (".into()); }
                let mut i = 11; let mut preds = vec![];
                loop {
                    let col = at(i)?.clone(); tbl.schema.col(&col).ok_or(format!("no such column {col}"))?;
                    let cmp = match at(i + 1)?.as_str() { "=" => Cmp::Eq, "<>" => Cmp::Ne, "<" => Cmp::Lt, "<=" => Cmp::Le, ">" => Cmp::Gt, ">=" => Cmp::Ge, x => return Err(format!("bad operator {x}")) };
                    let r = at(i + 2)?; let rhs = if let Some(a) = r.strip_prefix("SESSION.").or_else(|| r.strip_prefix("session.")) { PolicyRhs::Attr(a.to_string()) } else { PolicyRhs::Lit(literal_tok(r)?) };
                    preds.push((col, cmp, rhs)); i += 3;
                    if up.get(i).map(|x| x.as_str()) == Some("AND") { i += 1; continue; }
                    if t.get(i).map(|x| x.as_str()) == Some(")") { break; }
                    return Err(format!("unexpected token {:?}", t.get(i)));
                }
                acl.policies.push(Policy { name, table, role, cmds, preds }); Ok(())
            }
            _ => Err(format!("unsupported admin statement: {sql}")),
        }
    }
    /// Privilege check + compiled row policies for this session on a table. None = unrestricted session.
    fn guard(&self, sess: &Session, table: &Table, cmd: u8) -> Result<Vec<CPred>, String> {
        let user = match &sess.user { None => return Ok(vec![]), Some(u) => u };
        let acl = self.acl.read().unwrap();
        let tname = table.schema.name.to_lowercase();
        let allowed = sess.roles.iter().any(|r| acl.grants.get(&(r.clone(), tname.clone())).map(|m| m & cmd != 0).unwrap_or(false));
        if !allowed { return Err(format!("permission denied: user {user} lacks {} on {}", match cmd { P_SELECT => "SELECT", P_INSERT => "INSERT", P_DELETE => "DELETE", _ => "UPDATE" }, table.schema.name)); }
        let mut out = vec![];
        for p in acl.policies.iter().filter(|p| p.table == tname && p.cmds & cmd != 0 && sess.roles.contains(&p.role)) {
            for (col, cmp, rhs) in &p.preds {
                let v = match rhs { PolicyRhs::Lit(v) => v.clone(), PolicyRhs::Attr(a) => sess.attrs.get(a).cloned().ok_or(format!("policy {} needs session attribute {a}", p.name))? };
                let c = table.schema.col(col).unwrap();
                out.push(self.compile(sess, &table.schema, &Pred { col: c, cmp: *cmp, rhs: Expr::Lit(v) }, &[])?);
            }
        }
        Ok(out)
    }
    pub fn prepare(&self, sql: &str) -> Result<Arc<Prepared>, String> {
        if let Some(p) = self.cache.lock().unwrap().get(sql) { return Ok(p.clone()); }
        let stmts = Parser::parse_sql(&GenericDialect {}, sql).map_err(|e| e.to_string())?;
        if stmts.len() != 1 { return Err("one statement at a time".into()); }
        let mut pl = Planner { db: &self.db, nparams: 0, outer: vec![], cur: vec![], corr: None, exists_mode: false };
        let plan = pl.plan(&stmts[0])?;
        let p = Arc::new(Prepared { plan, nparams: pl.nparams, text: sql.to_string(), epoch: self.epoch.load(std::sync::atomic::Ordering::Acquire) });
        if !matches!(p.plan, Plan::CreateTable(_) | Plan::Begin | Plan::Commit | Plan::Rollback | Plan::Alter { .. }) { self.cache.lock().unwrap().insert(sql.to_string(), p.clone()); }
        Ok(p)
    }
    pub fn exec(&self, sql: &str, params: &[Value]) -> Result<Vec<Vec<Value>>, String> { let p = self.prepare(sql)?; self.execute(&p, params) }
    pub fn exec_s(&self, sess: &mut Session, sql: &str, params: &[Value]) -> Result<Vec<Vec<Value>>, String> { if is_admin_stmt(sql) { self.admin(sess, sql)?; self.log_meta(sql); return Ok(vec![]); } let p = self.prepare(sql)?; self.execute_s(sess, &p, params) }
    pub fn execute(&self, p: &Prepared, params: &[Value]) -> Result<Vec<Vec<Value>>, String> { let mut s = Session::default(); self.execute_s(&mut s, p, params) }

    fn resolve(&self, sess: &Session, e: &Expr, params: &[Value]) -> Result<Value, String> {
        match e {
            Expr::Lit(v) => Ok(v.clone()),
            Expr::Param(i) => params.get(*i).cloned().ok_or(format!("missing parameter {}", i + 1)),
            Expr::Scalar(sp) => { let rows = self.run_select(sess, sp, params)?; Ok(rows.first().and_then(|r| r.first().cloned()).unwrap_or(Value::Null)) }
            _ => Err("a list or subquery is not a value".into()),
        }
    }
    fn compile(&self, sess: &Session, schema: &Schema, p: &Pred, params: &[Value]) -> Result<CPred, String> {
        let off = schema.offsets[p.col]; let c = &schema.columns[p.col];
        let (nb, nm) = (p.col >> 3, 1u8 << (p.col & 7));
        let base = CPred { off, kind: 0, cmp: p.cmp, i: 0, f: 0.0, b: vec![], nb, nm, set: None, pair: None, corr_map: None, ckind: 0 };
        if let Expr::Scalar(sp) = &p.rhs { if let Some((_, oc)) = sp.corr {
            if !matches!(c.ty, ColType::Int | ColType::Float) { return Err(format!("column {}: correlated comparison needs a numeric column", c.name)); }
            let rows = self.run_select(sess, sp, params)?;
            let mut map: FastMap<f64> = FastMap::default();
            for r in rows { if let Value::Int(k) = r[0] { if let Ok(f) = r[1].as_f64() { map.insert(k, f); } } }
            return Ok(CPred { kind: 20, pair: Some((schema.offsets[oc], oc >> 3, 1u8 << (oc & 7))), corr_map: Some(Arc::new(map)), ckind: if c.ty == ColType::Float { 1 } else { 0 }, ..base });
        } }
        if matches!(p.cmp, Cmp::IsNull | Cmp::IsNotNull) { return Ok(base); }
        if matches!(p.cmp, Cmp::In | Cmp::NotIn) {
            if c.ty != ColType::Int { return Err(format!("column {}: IN is supported on INT columns (for now)", c.name)); }
            let mut pair = None;
            let keys: Vec<i64> = match &p.rhs {
                Expr::List(items) => items.iter().map(|e| self.resolve(sess, e, params)).collect::<Result<Vec<_>, _>>()?.into_iter().filter_map(|v| if let Value::Int(i) = v { Some(i) } else { None }).collect(),
                Expr::Set(sp) => { let rows = self.run_select(sess, sp, params)?; match sp.corr {
                    None => rows.into_iter().filter_map(|r| if let Some(Value::Int(i)) = r.first() { Some(*i) } else { None }).collect(),
                    Some((_, oc)) => if sp.proj.len() == 1 { rows.into_iter().filter_map(|r| if let Some(Value::Int(i)) = r.first() { Some(*i) } else { None }).collect() }   // EXISTS: the set of correlated keys
                                     else { pair = Some((schema.offsets[oc], oc >> 3, 1u8 << (oc & 7))); rows.into_iter().filter_map(|r| match (&r[0], &r[1]) { (Value::Int(a), Value::Int(b)) => Some(mix_pair(*a, *b)), _ => None }).collect() } } }
                _ => return Err("IN needs a list or a subquery".into()),
            };
            return Ok(CPred { set: Some(Arc::new(KeySet::build(&keys))), pair, ..base });
        }
        let v = self.resolve(sess, &p.rhs, params)?;
        if v == Value::Null { return Ok(CPred { kind: 9, ..base }); } // comparison with NULL is never true
        Ok(match &c.ty {
            ColType::Int => CPred { kind: 0, i: v.as_i64().map_err(|e| format!("column {}: {e}", c.name))?, ..base },
            ColType::Float => CPred { kind: 1, f: v.as_f64().map_err(|e| format!("column {}: {e}", c.name))?, ..base },
            ColType::Bool => CPred { kind: 2, i: match v { Value::Bool(b) => b as i64, v => return Err(format!("column {}: expected BOOL, got {v:?}", c.name)) }, ..base },
            ColType::Text(_) => CPred { kind: 3, b: match v { Value::Text(s) => s.into_bytes(), v => return Err(format!("column {}: expected TEXT, got {v:?}", c.name)) }, ..base },
            ColType::Bytes(_) => CPred { kind: 3, b: match v { Value::Bytes(b) => b, v => return Err(format!("column {}: expected BYTES, got {v:?}", c.name)) }, ..base },
        })
    }
    /// Run a (sub)select against committed data with this session's privileges.
    fn run_select(&self, sess: &Session, s: &SelectPlan, params: &[Value]) -> Result<Vec<Vec<Value>>, String> {
        let pol = match &s.join { Some(j) => { let mut v = vec![self.guard(sess, &j.probe, P_SELECT)?]; for st in &j.steps { v.push(self.guard(sess, &st.build, P_SELECT)?); } v }, None => vec![self.guard(sess, &s.table, P_SELECT)?] };
        self.select(sess, s, params, None, pol)
    }

    pub fn execute_s(&self, sess: &mut Session, p: &Prepared, params: &[Value]) -> Result<Vec<Vec<Value>>, String> {
        if params.len() < p.nparams { return Err(format!("statement needs {} parameters, got {}", p.nparams, params.len())); }
        if p.epoch != self.epoch.load(std::sync::atomic::Ordering::Acquire) { let p2 = self.prepare(&p.text)?; return self.execute_s(sess, &p2, params); }  // a schema change swapped the table under this plan
        match &p.plan {
            Plan::Alter { table, ops } => { if sess.user.is_some() { return Err("permission denied: only the application session can alter tables".into()); } self.alter(&p.text, table, ops, true)?; Ok(vec![]) }
            Plan::CreateTable(s) => { if sess.user.is_some() { return Err("permission denied: only the application session can create tables".into()); } self.db.create_table(s.clone())?; self.log_meta(&p.text); Ok(vec![]) }
            Plan::Begin => { if sess.txn.is_some() { return Err("already in a transaction".into()); } sess.txn = Some(Txn::new()); Ok(vec![]) }
            Plan::Rollback => { sess.txn = None; Ok(vec![]) }
            Plan::Commit => { let t = sess.txn.take().ok_or("not in a transaction")?; self.db.commit(t).map_err(|a| a.to_string())?; Ok(vec![]) }
            Plan::Insert { table, exprs } => {
                let policy = self.guard(sess, table, P_INSERT)?;
                let vals = exprs.iter().map(|e| self.resolve(sess, e, params)).collect::<Result<Vec<_>, _>>()?;
                let row = encode_row(&table.schema, &vals)?;
                for p in &policy { if !p.eval(&row) { return Err(format!("permission denied: row violates a policy on {}", table.schema.name)); } }
                match &mut sess.txn { Some(t) => table.txn_insert(t, &row), None => table.insert(&row).map(|_| ()) }.map_err(|a| a.to_string())?; Ok(vec![])
            }
            Plan::Update { table, sets, pk } => {
                let policy = self.guard(sess, table, P_UPDATE)?;
                let pk = self.resolve(sess, pk, params)?.as_i64()?;
                let schema = &table.schema;
                let vals = sets.iter().map(|(c, e)| self.resolve(sess, e, params).map(|v| (*c, v))).collect::<Result<Vec<_>, _>>()?;
                let f = |row: &[u8]| -> Result<Vec<u8>, Abort> { for p in &policy { if !p.eval(row) { return Err(Abort::NotFound); } } let mut new = row.to_vec(); for (c, v) in &vals { encode_col(schema, &mut new, *c, v).map_err(Abort::Type)?; } for p in &policy { if !p.eval(&new) { return Err(Abort::Type("permission denied: new row violates a policy".into())); } } Ok(new) };
                match &mut sess.txn { Some(t) => table.txn_update(t, pk, f), None => table.update_by_pk(pk, f).map(|_| ()) }.map_err(|a| a.to_string())?;
                Ok(vec![])
            }
            Plan::Delete { table, access, preds } => {
                let mut policy = self.guard(sess, table, P_DELETE)?;
                let schema = &table.schema;
                let mut cp = preds.iter().map(|p| self.compile(sess, schema, p, params)).collect::<Result<Vec<_>, _>>()?; cp.append(&mut policy);
                let pk_off = schema.offsets[schema.pk];
                let mut pks: Vec<i64> = vec![];
                let mut collect = |row: &[u8]| -> bool { for p in &cp { if !p.eval(row) { return true; } } pks.push(read_i64(row, pk_off)); true };
                match access {
                    Access::Pk(e) => { let pk = self.resolve(sess, e, params)?.as_i64()?; match &mut sess.txn { Some(t) => { if let Some(r) = table.txn_get(t, pk) { collect(&r); } } None => { table.with_pk(pk, |r| collect(r)); } } }
                    Access::Unique(e) => { let u = schema.unique.unwrap(); let ty = &schema.columns[u].ty; let v = self.resolve(sess, e, params)?; let mut key = vec![0u8; ty.width()]; encode_value(ty, &v, &mut key)?; table.with_unique(&key, |r| collect(r)); }
                    Access::Range { lo, lo_excl, hi, hi_excl } => { let mut a = self.resolve(sess, lo, params)?.as_i64()?; let mut b = self.resolve(sess, hi, params)?.as_i64()?; if *lo_excl { a += 1; } if *hi_excl { b -= 1; } table.range(a, b, &mut collect); }
                    Access::Index(ix, e) => { let k = self.resolve(sess, e, params)?.as_i64()?; table.index_lookup(ix, k, &mut collect); }
                    Access::Scan => table.scan(&mut collect),
                }
                let n = pks.len();
                match &mut sess.txn {
                    Some(t) => { for pk in pks { table.txn_delete(t, pk).map_err(|a| a.to_string())?; } }
                    None => if n == 1 { table.delete_by_pk(pks[0]).map_err(|a| a.to_string())?; } else if n > 1 { let mut t = Txn::new(); for pk in pks { table.txn_delete(&mut t, pk).map_err(|a| a.to_string())?; } self.db.commit(t).map_err(|a| a.to_string())?; }
                }
                Ok(vec![vec![Value::Int(n as i64)]])
            }
            Plan::Select(s) => { let pol = match &s.join { Some(j) => { let mut v = vec![self.guard(sess, &j.probe, P_SELECT)?]; for st in &j.steps { v.push(self.guard(sess, &st.build, P_SELECT)?); } v }, None => vec![self.guard(sess, &s.table, P_SELECT)?] }; let (txn, sref) = (sess.txn.take(), &*sess); let r = { let mut txn = txn; let r = self.select(sref, s, params, txn.as_mut(), pol); (r, txn) }; sess.txn = r.1; r.0 }
        }
    }

    fn select(&self, sess: &Session, s: &SelectPlan, params: &[Value], txn: Option<&mut Txn>, mut policies: Vec<Vec<CPred>>) -> Result<Vec<Vec<Value>>, String> {
        let schema = &s.schema;
        let mut preds = s.preds.iter().map(|p| self.compile(sess, schema, p, params)).collect::<Result<Vec<_>, _>>()?;
        let mut subs: Vec<SubVal> = Vec::with_capacity(s.subs.len());
        let outer_selective = (!matches!(s.access, Access::Scan) || !s.preds.is_empty()) && s.join.is_none();
        for (sp, oc) in &s.subs {
            let dflt = if matches!(sp.proj.get(1), Some(Proj::Agg(Agg::CountStar | Agg::CountCol(_)))) { Value::Int(0) } else { Value::Null };
            // selective outer side + an index on the inner correlation column: evaluate the inner per outer key through the index
            if let (Some(oc), Some((cv, _)), None) = (oc, sp.corr, &sp.join) {
                if let (true, Some(ix), Some(Proj::Agg(agg))) = (outer_selective, sp.table.index_on(cv), sp.proj.get(1)) {
                    let pi = params.len();
                    let lazy = Arc::new(SelectPlan { table: sp.table.clone(), schema: sp.schema.clone(), access: Access::Index(ix, Expr::Param(pi)), preds: sp.preds.clone(), proj: vec![Proj::Agg(agg.clone())], group_by: None, order: None, limit: None, join: None, corr: None, subs: vec![] });
                    let base: Vec<Value> = params.to_vec(); let dflt2 = dflt.clone();
                    let f = move |k: i64| -> Value { let mut p = base.clone(); p.push(Value::Int(k)); match self.run_select(sess, &lazy, &p) { Ok(rows) => rows.first().and_then(|r| r.first().cloned()).map(|v| if v == Value::Null { dflt2.clone() } else { v }).unwrap_or_else(|| dflt2.clone()), Err(_) => Value::Null } };
                    let sp2 = sp.clone(); let params2: Vec<Value> = params.to_vec();
                    let full = move || -> FastMap<Value> { let mut m = FastMap::default(); if let Ok(rows) = self.run_select(sess, &sp2, &params2) { for r in rows { if let Value::Int(k) = r[0] { m.insert(k, r[1].clone()); } } } m };
                    subs.push(SubVal::Lazy { f: Box::new(f), full: Box::new(full), dflt, off: schema.offsets[*oc], nb: oc >> 3, nm: 1u8 << (oc & 7), cache: std::cell::RefCell::new(FastMap::default()), fallback: std::cell::RefCell::new(None) });
                    continue;
                }
            }
            let rows = self.run_select(sess, sp, params)?;
            subs.push(match oc {
                None => SubVal::Const(rows.first().and_then(|r| r.first().cloned()).unwrap_or(Value::Null)),
                Some(oc) => { let mut map: FastMap<Value> = FastMap::default(); for r in rows { if let Value::Int(k) = r[0] { map.insert(k, r[1].clone()); } } SubVal::Map { map, off: schema.offsets[*oc], nb: oc >> 3, nm: 1u8 << (oc & 7), dflt } }
            });
        }
        let mut sink = Sink::new(schema, &s.proj, s.group_by, s.order, s.limit, subs);
        if let Some(j) = &s.join { self.run_joins(sess, j, params, &preds, &mut sink, policies)?; return Ok(sink.finish()); }
        preds.append(&mut policies.pop().unwrap_or_default());
        let t = &s.table;
        let covering = preds.is_empty() && sink.count_only() && sink.group_by.is_none();
        let mut visit = |row: &[u8]| -> bool { for p in &preds { if !p.eval(row) { return true; } } sink.push(row) };
        match &s.access {
            Access::Pk(e) => { let pk = self.resolve(sess, e, params)?.as_i64()?; match txn { Some(tx) => { if let Some(row) = t.txn_get(tx, pk) { visit(&row); } } None => { t.with_pk(pk, |r| visit(r)); } } }
            Access::Unique(e) => { let u = schema.unique.unwrap(); let ty = &schema.columns[u].ty; let v = self.resolve(sess, e, params)?; let mut key = vec![0u8; ty.width()]; encode_value(ty, &v, &mut key)?; t.with_unique(&key, |r| visit(r)); }
            Access::Range { lo, lo_excl, hi, hi_excl } => { let mut a = self.resolve(sess, lo, params)?.as_i64()?; let mut b = self.resolve(sess, hi, params)?.as_i64()?; if *lo_excl { a += 1; } if *hi_excl { b -= 1; } t.range(a, b, &mut visit); }
            Access::Index(ix, e) => { let k = self.resolve(sess, e, params)?.as_i64()?; if covering { let n = ix.count(k) as u64; drop(visit); sink.push_count(&[], n); return Ok(sink.finish()); } t.index_lookup(ix, k, &mut visit); }
            Access::Scan => t.scan(&mut visit),
        }
        Ok(sink.finish())
    }

    /// Filter one join step's build table; returns (keys, compact rows [flags][cols], stride).
    fn collect_build(&self, sess: &Session, st: &JoinStep, params: &[Value], bp: &[CPred]) -> Result<(Vec<i64>, Vec<u8>, usize), String> {
        let bs = &st.build.schema; let bkey_off = bs.offsets[st.build_key];
        let cols: Vec<(usize, usize, usize)> = st.build_cols.iter().map(|&c| (bs.offsets[c], bs.columns[c].ty.width(), c)).collect();
        let stride = cols.len() + cols.iter().map(|c| c.1).sum::<usize>();
        let mut keys: Vec<i64> = Vec::new(); let mut arena: Vec<u8> = Vec::new();
        let mut collect = |row: &[u8]| -> bool { for p in bp { if !p.eval(row) { return true; } } keys.push(read_i64(row, bkey_off)); for (_, _, c) in &cols { arena.push(is_null(row, *c) as u8); } for (off, w, _) in &cols { arena.extend_from_slice(&row[*off..*off + *w]); } true };
        match &st.build_access {
            Access::Pk(e) => { let pk = self.resolve(sess, e, params)?.as_i64()?; st.build.with_pk(pk, |r| collect(r)); }
            Access::Range { lo, lo_excl, hi, hi_excl } => { let mut a = self.resolve(sess, lo, params)?.as_i64()?; let mut b = self.resolve(sess, hi, params)?.as_i64()?; if *lo_excl { a += 1; } if *hi_excl { b -= 1; } st.build.range(a, b, &mut collect); }
            Access::Index(ix, e) => { let k = self.resolve(sess, e, params)?.as_i64()?; st.build.index_lookup(ix, k, &mut collect); }
            _ => st.build.scan(&mut collect),
        }
        Ok((keys, arena, stride))
    }
    /// Key-addressable store over collected build rows: bitmap (no columns), dense array, or hash map.
    fn make_store(keys: &[i64], arena: Vec<u8>, stride: usize) -> Store {
        let n = keys.len();
        if n == 0 { return Store::Empty; }
        let (min, max) = keys.iter().fold((i64::MAX, i64::MIN), |(a, b), &k| (a.min(k), b.max(k)));
        let range = (max - min) as u64 + 1;
        if stride == 0 { return Store::Set(KeySet::build(keys)); }
        if range <= 16_000_000 {
            let es = stride + 1; let mut dense = vec![0u8; range as usize * es];
            for (i, &k) in keys.iter().enumerate() { let o = (k - min) as usize * es; dense[o] = 1; dense[o + 1..o + es].copy_from_slice(&arena[i * stride..(i + 1) * stride]); }
            return Store::Dense { min, max, es, data: dense };
        }
        let mut map = FastMap::default(); map.reserve(n); for (i, &k) in keys.iter().enumerate() { map.insert(k, i as u32); }
        Store::Hash { map, arena, stride }
    }

    fn run_joins(&self, sess: &Session, j: &JoinPlan, params: &[Value], preds: &[CPred], sink: &mut Sink, mut policies: Vec<Vec<CPred>>) -> Result<(), String> {
        let ps = &j.probe.schema; let vschema: &Schema = sink.schema;
        let vn = vschema.null_bytes; let pbase = vn;
        let mut pp = j.probe_preds.iter().map(|p| self.compile(sess, ps, p, params)).collect::<Result<Vec<_>, _>>()?; pp.append(&mut policies[0]);
        let mut bps: Vec<Vec<CPred>> = vec![];
        for (k, st) in j.steps.iter().enumerate() { let mut v = st.build_preds.iter().map(|p| self.compile(sess, &st.build.schema, p, params)).collect::<Result<Vec<_>, _>>()?; v.append(&mut policies[k + 1]); bps.push(v); }
        let layouts: Vec<Vec<(usize, usize, usize)>> = j.steps.iter().map(|st| st.build_cols.iter().map(|&c| (st.build.schema.offsets[c], st.build.schema.columns[c].ty.width(), c)).collect()).collect();
        let key_offs: Vec<usize> = j.steps.iter().map(|st| vschema.offsets[st.key_vidx]).collect();
        let mut combined = vec![0u8; vschema.row_size];
        let fill_probe = |combined: &mut [u8], prow: &[u8]| { for b in &mut combined[..vn] { *b = 0; } combined[..ps.null_bytes].copy_from_slice(&prow[..ps.null_bytes]); combined[pbase..pbase + ps.row_size].copy_from_slice(prow); };
        let fill_build = |combined: &mut [u8], k: usize, brow: &[u8]| { let st = &j.steps[k]; let mut o = st.base; for (i, (off, w, c)) in layouts[k].iter().enumerate() { combined[o..o + w].copy_from_slice(&brow[*off..*off + *w]); if is_null(brow, *c) { let vi = st.vbase + i; combined[vi >> 3] |= 1 << (vi & 7); } o += w; } };
        let fill_entry = |combined: &mut [u8], k: usize, entry: &[u8]| { let st = &j.steps[k]; let nf = layouts[k].len(); combined[st.base..st.base + entry.len() - nf].copy_from_slice(&entry[nf..]); for i in 0..nf { if entry[i] != 0 { let vi = st.vbase + i; combined[vi >> 3] |= 1 << (vi & 7); } } };
        // Point mode: the probe side is selective, every step is a primary-key lookup.
        // Scan mode: filter every build side first; if one filtered side is small and the probe key is indexed, drive
        // the join through that index (expected probe rows = matched/total * probe rows); otherwise scan the probe.
        let point_mode = !matches!(j.probe_access, Access::Scan);
        let mut stores: Vec<Store> = vec![]; let mut driver: Option<(usize, Vec<i64>, Vec<u8>, usize, Arc<SecIndex>)> = None;
        if !point_mode {
            let mut collected: Vec<(Vec<i64>, Vec<u8>, usize)> = vec![];
            for (k, st) in j.steps.iter().enumerate() { collected.push(self.collect_build(sess, st, params, &bps[k])?); }
            let prow_n = j.probe.rows_allocated().max(1) as f64;
            let mut best = f64::MAX;
            for (k, st) in j.steps.iter().enumerate() {
                if st.key_vidx >= ps.columns.len() { continue; }
                if let Some(ix) = j.probe.index_on(st.key_vidx) {
                    let (m, b) = (collected[k].0.len() as f64, st.build.rows_allocated().max(1) as f64);
                    let expected = m / b * prow_n;                     // probe rows reached through the index
                    if expected * 40.0 < prow_n && expected < best { best = expected; driver = Some((k, vec![], vec![], 0, ix)); }   // an index-reached row costs ~40 scanned rows
                }
            }
            for (k, (keys, arena, stride)) in collected.into_iter().enumerate() {
                if let Some(d) = &mut driver { if d.0 == k { d.1 = keys; d.2 = arena; d.3 = stride; stores.push(Store::Empty); continue; } }
                stores.push(Self::make_store(&keys, arena, stride));
            }
        }
        if let Some((k, keys, arena, stride, ix)) = &driver {
            if pp.is_empty() && preds.is_empty() && j.steps.len() == 1 && sink.count_only() && sink.group_at_or_after(ps.columns.len()) {
                // covering: the index entry count per key IS the number of committed matching rows; no probe rows are read
                for (i, key) in keys.iter().enumerate() { let n = ix.count(*key) as u64; if n > 0 { for b in &mut combined[..vn] { *b = 0; } fill_entry(&mut combined, *k, &arena[i * stride..(i + 1) * stride]); sink.push_count(&combined, n); } }
                return Ok(());
            }
        }
        let apply_step = |combined: &mut [u8], k: usize| -> bool {
            let st = &j.steps[k];
            if is_null(combined, st.key_vidx) { return false; }
            let key = read_i64(combined, key_offs[k]);
            if point_mode { return st.build.with_pk(key, |brow| { for p in &bps[k] { if !p.eval(brow) { return false; } } fill_build(combined, k, brow); true }).unwrap_or(false); }
            match &stores[k] {
                Store::Empty => false,
                Store::Set(set) => set.contains(key),
                Store::Dense { min, max, es, data } => { if key < *min || key > *max { return false; } let o = (key - min) as usize * es; if data[o] == 0 { return false; } fill_entry(combined, k, &data[o + 1..o + es]); true }
                Store::Hash { map, arena, stride } => match map.get(&key) { Some(&i) => { fill_entry(combined, k, &arena[i as usize * stride..(i as usize + 1) * stride]); true } None => false },
            }
        };
        let stop = std::cell::Cell::new(false);
        let mut finish = |combined: &mut [u8], skip: usize| -> bool {
            for k in 0..j.steps.len() { if k == skip { continue; } if !apply_step(combined, k) { return true; } }
            for p in preds { if !p.eval(combined) { return true; } }
            if !sink.push(combined) { stop.set(true); return false; }
            true
        };
        if let Some((k, keys, arena, stride, ix)) = driver {
            for (i, key) in keys.iter().enumerate() {
                let entry = &arena[i * stride..(i + 1) * stride];
                j.probe.index_lookup(&ix, *key, |prow| { for p in &pp { if !p.eval(prow) { return true; } } fill_probe(&mut combined, prow); fill_entry(&mut combined, k, entry); finish(&mut combined, k) });
                if stop.get() { break; }
            }
            return Ok(());
        }
        // reject on the raw probe row before copying anything: steps keyed by a probe column whose store is a plain key set
        let prechecks: Vec<(usize, usize, usize)> = j.steps.iter().enumerate().filter(|(k, st)| st.key_vidx < ps.columns.len() && matches!(stores.get(*k), Some(Store::Set(_)) | Some(Store::Empty))).map(|(k, st)| (k, st.key_vidx, ps.offsets[st.key_vidx])).collect();
        let mut per_probe = |prow: &[u8]| -> bool {
            for p in &pp { if !p.eval(prow) { return true; } }
            for &(k, col, off) in &prechecks { if is_null(prow, col) { return true; } match &stores[k] { Store::Set(set) => if !set.contains(read_i64(prow, off)) { return true; }, Store::Empty => return true, _ => {} } }
            fill_probe(&mut combined, prow); finish(&mut combined, usize::MAX)
        };
        match &j.probe_access {
            Access::Pk(e) => { let pk = self.resolve(sess, e, params)?.as_i64()?; j.probe.with_pk(pk, |r| per_probe(r)); }
            Access::Unique(e) => { let u = ps.unique.unwrap(); let ty = &ps.columns[u].ty; let v = self.resolve(sess, e, params)?; let mut key = vec![0u8; ty.width()]; encode_value(ty, &v, &mut key)?; j.probe.with_unique(&key, |r| per_probe(r)); }
            Access::Range { lo, lo_excl, hi, hi_excl } => { let mut a = self.resolve(sess, lo, params)?.as_i64()?; let mut b = self.resolve(sess, hi, params)?.as_i64()?; if *lo_excl { a += 1; } if *hi_excl { b -= 1; } j.probe.range(a, b, &mut per_probe); }
            Access::Index(ix, e) => { let k = self.resolve(sess, e, params)?.as_i64()?; j.probe.index_lookup(ix, k, &mut per_probe); }
            Access::Scan => j.probe.scan(&mut per_probe),
        }
        Ok(())
    }
}
enum Store { Empty, Set(KeySet), Dense { min: i64, max: i64, es: usize, data: Vec<u8> }, Hash { map: FastMap<u32>, arena: Vec<u8>, stride: usize } }

