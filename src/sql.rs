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

#[derive(Clone)] pub enum Expr { Lit(Value), Param(usize), List(Vec<Expr>), Set(Arc<SelectPlan>), Scalar(Arc<SelectPlan>), /// INSERT: the next automatic primary key
    AutoId, /// INSERT: a value computed per row (`uuid()`, `now()`)
    Gen(crate::table::Gen) }
impl std::fmt::Debug for Expr { fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { match self { Expr::Lit(v) => write!(f, "{v:?}"), Expr::Param(i) => write!(f, "?{i}"), Expr::AutoId => write!(f, "auto"), Expr::Gen(g) => write!(f, "{}", g.sql()), Expr::List(l) => write!(f, "list({})", l.len()), Expr::Set(_) => write!(f, "(subquery)"), Expr::Scalar(_) => write!(f, "(scalar subquery)") } } }
#[derive(Clone, Copy, Debug, PartialEq)] pub enum Cmp { Eq, Ne, Lt, Le, Gt, Ge, IsNull, IsNotNull, In, NotIn }
#[derive(Clone, Debug)] pub struct Pred { pub col: usize, pub cmp: Cmp, pub rhs: Expr }
#[derive(Clone, Debug)] pub enum Agg { CountStar, CountCol(usize), Sum(usize), Avg(usize), Min(usize), Max(usize) }
#[derive(Clone, Debug)] pub enum Proj { Col(usize), Agg(Agg), Sub(usize) }
#[derive(Clone)] pub enum Access { Pk(Expr), Unique(Expr), Range { lo: Expr, lo_excl: bool, hi: Expr, hi_excl: bool }, Index(Arc<SecIndex>, Expr), Scan }
/// Equi-join chain. The probe table is scanned (or reached by key/index); each step joins one more table whose join
/// column is its primary key, copying that table's needed columns next to the row, so the pipeline sees one virtual row.
pub struct JoinStep { pub build: Arc<Table>, pub build_key: usize, pub key_vidx: usize, pub build_preds: Vec<Pred>, pub build_cols: Vec<usize>, pub base: usize, pub vbase: usize, pub build_access: Access }
pub struct JoinPlan { pub probe: Arc<Table>, pub probe_access: Access, pub probe_preds: Vec<Pred>, pub steps: Vec<JoinStep>, pub index_first: Option<Arc<SecIndex>> }
pub struct SelectPlan { pub table: Arc<Table>, pub schema: Schema, pub access: Access, pub preds: Vec<Pred>, pub proj: Vec<Proj>, pub names: Vec<String>, pub group_by: Option<usize>, pub order: Option<(usize, bool)>, pub limit: Option<usize>, pub join: Option<JoinPlan>, pub corr: Option<(usize, usize)>, pub subs: Vec<(Arc<SelectPlan>, Option<usize>)> }
pub enum Plan { CreateTable(Schema), Insert { table: Arc<Table>, rows: Vec<Vec<Expr>>, returning: Option<Vec<usize>> }, Select(SelectPlan), Update { table: Arc<Table>, sets: Vec<(usize, Expr)>, pk: Expr }, Delete { table: Arc<Table>, access: Access, preds: Vec<Pred> }, Begin, Commit, Rollback, Alter { table: String, ops: Vec<AlterOp> } }
#[derive(Clone, Debug)] pub enum AlterOp { Add { col: Column, default: Value }, Drop(String), Rename(String, String), SetType(String, ColType), SetNotNull(String), DropNotNull(String) }
pub struct Prepared { pub plan: Plan, pub nparams: usize, pub text: String, pub epoch: u64 }
#[derive(Default)] pub struct Session { pub txn: Option<Txn>, pub user: Option<String>, pub roles: Vec<String>, pub attrs: HashMap<String, Value>,
    /// true when the identity came from the application (`assume`), not from a user stored in the database
    pub assumed: bool, /// sequence of this session's last commit (autocommit or COMMIT); `Sql::wait_durable` makes it durable
    pub last_seq: u64,
    /// primary key of the last row this session inserted (0 before any insert)
    pub last_insert_id: i64,
    /// the same key as a value: an INT, or a UUID as text (None before any insert)
    pub last_insert_key: Option<Value> }

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
pub fn is_admin_stmt(sql: &str) -> bool { let u = sql.trim_start().to_uppercase(); u.starts_with("CREATE ROLE ") || u.starts_with("CREATE USER ") || u.starts_with("GRANT ") || u.starts_with("CREATE POLICY ") || u.starts_with("CREATE INDEX ") }

fn obj_name(n: &ObjectName) -> String { n.0.iter().map(|p| match p { ObjectNamePart::Identifier(i) => i.value.clone(), other => format!("{other}") }).collect::<Vec<_>>().join(".") }
/// A column default: a constant, or a generator evaluated per row. A constant is checked against the column type now.
fn default_spec(e: &ast::Expr, col: &str, ty: &ColType) -> Result<(Value, Option<crate::table::Gen>), String> {
    use crate::table::Gen;
    if let ast::Expr::Function(f) = e {
        let g = match obj_name(&f.name).to_lowercase().as_str() {
            "uuid" | "uuid_v7" | "uuidv7" | "uuid_generate_v7" => Gen::Uuid7,
            "gen_random_uuid" | "uuid_v4" | "uuidv4" | "uuid_generate_v4" | "random_uuid" => Gen::Uuid4,
            "now" | "current_timestamp" | "localtimestamp" => Gen::Now,
            other => return Err(format!("column {col}: unsupported default {other}() (generated defaults: uuid(), gen_random_uuid(), now())")),
        };
        let fits = matches!((&g, ty), (Gen::Uuid7 | Gen::Uuid4, ColType::Uuid) | (Gen::Now, ColType::Timestamp));
        if !fits { return Err(format!("column {col}: DEFAULT {} needs a {} column, not {}", g.sql(), if g == Gen::Now { "TIMESTAMP" } else { "UUID" }, ty.name())); }
        return Ok((Value::Null, Some(g)));
    }
    let v = default_value(e)?;
    if v != Value::Null { let mut buf = vec![0u8; ty.width()]; encode_value(ty, &v, &mut buf).map_err(|e| format!("column {col}: DEFAULT {e}"))?; }
    Ok((v, None))
}
/// SERIAL and friends: an INT column whose values are assigned automatically.
fn is_serial(dt: &ast::DataType) -> bool { matches!(format!("{dt}").to_uppercase().as_str(), "SERIAL" | "BIGSERIAL" | "SMALLSERIAL" | "SERIAL4" | "SERIAL8") }
/// The key an index lookup uses for a value of column `col`: an INT, a TIMESTAMP (number or text) or a UUID (text or 16 bytes).
fn index_key(schema: &Schema, col: usize, v: &Value) -> Result<crate::table::Key, String> {
    let c = &schema.columns[col];
    match (&c.ty, v) {
        (ColType::Uuid, Value::Text(s)) => crate::table::parse_uuid(s).map(|b| crate::table::key_from_uuid(&b)).ok_or_else(|| format!("column {}: not a UUID: {s:?}", c.name)),
        (ColType::Uuid, Value::Bytes(b)) if b.len() == 16 => Ok(crate::table::key_from_uuid(b)),
        (ColType::Uuid, v) => Err(format!("column {}: expected a UUID, got {v:?}", c.name)),
        (ColType::Timestamp, Value::Text(s)) => crate::table::parse_timestamp(s).map(|t| t as i128).ok_or_else(|| format!("column {}: not a timestamp {s:?}", c.name)),
        (_, v) => v.as_i64().map(|i| i as i128).map_err(|e| format!("column {}: {e}", c.name)),
    }
}
fn default_value(e: &ast::Expr) -> Result<Value, String> {
    match e {
        ast::Expr::Value(v) => match &v.value { ast::Value::Number(s, _) => Ok(if let Ok(i) = s.parse::<i64>() { Value::Int(i) } else { Value::Float(s.parse::<f64>().map_err(|e| e.to_string())?) }), ast::Value::SingleQuotedString(s) => Ok(Value::Text(s.clone())), ast::Value::Boolean(b) => Ok(Value::Bool(*b)), ast::Value::Null => Ok(Value::Null), other => Err(format!("unsupported default {other}")) },
        ast::Expr::UnaryOp { op: UnaryOperator::Minus, expr } => match default_value(expr)? { Value::Int(i) => Ok(Value::Int(-i)), Value::Float(f) => Ok(Value::Float(-f)), v => Err(format!("bad default {v:?}")) },
        ast::Expr::Identifier(i) => literal_tok(&i.value),
        other => Err(format!("unsupported default {other}")),
    }
}
/// Output column names for a SELECT: an alias wins; otherwise the bare column name, unless that bare name appears
/// twice in the result (a join), in which case the qualified form the user wrote (or the plan's `alias.col`) stays.
fn finish_names(names: &mut Vec<String>, proj: &[Proj], schema: &Schema) {
    for (i, p) in proj.iter().enumerate() {
        if names.get(i).map(|n| n.is_empty()).unwrap_or(true) {
            let n = match p { Proj::Col(v) => schema.columns.get(*v).map(|c| c.name.clone()).unwrap_or_else(|| format!("col{i}")), _ => format!("col{i}") };
            if i < names.len() { names[i] = n } else { names.push(n) }
        }
    }
    let bare: Vec<String> = names.iter().enumerate().map(|(i, n)| if matches!(proj.get(i), Some(Proj::Col(_))) { n.rsplit('.').next().unwrap_or(n).to_string() } else { n.clone() }).collect();
    for i in 0..names.len() { if bare.iter().filter(|b| **b == bare[i]).count() == 1 { names[i] = bare[i].clone(); } }
}
/// A short machine-readable class for an error message, shared by every binding (`CONFLICT`, `DUPLICATE`, ...).
/// The message for an INSERT whose VALUES row has the wrong number of values: it names the columns and shows the fix.
fn values_count_error(schema: &Schema, table: &str, listed: Option<&[usize]>, got: usize, row: usize, rows: usize) -> String {
    let names = |ix: &[usize]| ix.iter().map(|&i| schema.columns[i].name.as_str()).collect::<Vec<_>>().join(", ");
    let count = |k: usize| if k == 1 { "1 value".to_string() } else { format!("{k} values") };
    let which = if rows > 1 { format!("row {} of VALUES", row + 1) } else { "VALUES".to_string() };
    if let Some(cols) = listed {
        return format!("INSERT names {} column{} ({}) but {which} gives {}", cols.len(), if cols.len() == 1 { "" } else { "s" }, names(cols), count(got));
    }
    let all: Vec<usize> = (0..schema.columns.len()).collect();
    let mut msg = format!("table {table} has {} columns ({}) but {which} gives {}", all.len(), names(&all), count(got));
    if got < all.len() {
        // columns the database fills by itself: the automatic key and generated values (uuid(), now(), ...)
        let filled: Vec<usize> = all.iter().copied().filter(|&i| (i == schema.pk && schema.auto) || schema.columns[i].generated.is_some()).collect();
        let rest: Vec<usize> = all.iter().copied().filter(|i| !filled.contains(i)).collect();
        if !filled.is_empty() && rest.len() == got { msg += &format!("; name the columns you fill, as in INSERT INTO {table} ({}) VALUES (...), or write DEFAULT for {}", names(&rest), names(&filled)); }
        else { msg += &format!("; name the columns you fill, as in INSERT INTO {table} (column, ...) VALUES (...), and leave out the ones with a default"); }
    }
    msg
}
pub fn error_code(msg: &str) -> &'static str {
    let m = msg.trim_start();
    if m.starts_with("Conflict") { "CONFLICT" }
    else if m.contains(" VALUES gives ") { "SYNTAX" }
    else if m.starts_with("Duplicate") { "DUPLICATE" }
    else if m.starts_with("NotFound") { "NOT_FOUND" }
    else if m.starts_with("foreign key violation") || m.contains(" blocked: ") { "FOREIGN_KEY" }
    else if m.starts_with("permission denied") { "DENIED" }
    else if m.starts_with("statement needs ") { "ARGUMENTS" }
    else if m.contains("is not a sqweezdb database") || m.contains("a sqweezdb database is a directory") || m.contains("written by a newer sqweezdb") || m.contains("sqweezdb.meta") || m.contains("holds no sqweezdb database") { "PATH" }
    else if m.starts_with("options:") || m.contains(".toml") || m.contains(" is required") || m.starts_with("no database in") || m.starts_with("durable") || m.starts_with("follow") || m.starts_with("listen.") || m.starts_with("snapshot.") || m.starts_with("live.") || m.starts_with("log.") || m.starts_with("backup.") { "CONFIG" }
    else if m.starts_with("already in a transaction") || m.starts_with("not in a transaction") { "TRANSACTION" }
    else if m.contains("type mismatch") || m.contains(" exceeds ") || m.contains("is NOT NULL") || (m.starts_with("expected ") && m.contains(", got ")) || m.contains(": expected ") || m.contains("not a timestamp") || m.contains("not a UUID") || m.contains("expected a UUID") { "TYPE" }
    else if m.starts_with("sql parser error") || m.contains("Expected") || m.starts_with("expected ") || m.starts_with("unsupported") { "SYNTAX" }
    else if m.starts_with("no such") || m.starts_with("unknown") || m.contains(" no such ") { "SCHEMA" }
    else { "ERROR" }
}
fn coltype_of(dt: &ast::DataType) -> Result<ColType, String> {
    let s = format!("{dt}").to_uppercase();
    let num = |s: &str| s.split(|c| c == '(' || c == ')').nth(1).and_then(|x| x.trim().parse::<usize>().ok());
    Ok(match s.as_str() {
        "INT" | "INTEGER" | "BIGINT" | "INT64" | "SMALLINT" => ColType::Int,
        "UUID" => ColType::Uuid,
        "TIMESTAMP" | "DATETIME" | "TIMESTAMPTZ" => ColType::Timestamp,
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
                let mut auto: Option<usize> = None;
                for (i, c) in ct.columns.iter().enumerate() {
                    let mut nullable = true;
                    let mut default = Value::Null; let mut generated = None;
                    let ty = if is_serial(&c.data_type) { auto = Some(i); ColType::Int } else { coltype_of(&c.data_type)? };
                    for o in &c.options {
                        let s = format!("{}", o.option).to_uppercase();
                        if let ast::ColumnOption::ForeignKey(fk) = &o.option { fks.push(FkDef { col: i, parent: obj_name(&fk.foreign_table), on_delete: action(&fk.on_delete)? }); }
                        else if s.starts_with("PRIMARY KEY") { pk = Some(i); nullable = false; }
                        else if s.starts_with("UNIQUE") { unique = Some(i); }
                        else if s.starts_with("NOT NULL") { nullable = false; }
                        else if s.contains("AUTOINCREMENT") || s.contains("AUTO_INCREMENT") || matches!(&o.option, ast::ColumnOption::Generated { generation_expr: None, .. }) { auto = Some(i); }
                        else if let ast::ColumnOption::Default(e) = &o.option { (default, generated) = default_spec(e, &c.name.value, &ty)?; }
                    }
                    cols.push(Column { name: c.name.value.clone(), ty, nullable, default, generated });
                }
                for tc in &ct.constraints { if let ast::TableConstraint::ForeignKey(fk) = tc { if fk.columns.len() != 1 { return Err("one column per foreign key (for now)".into()); } let col = cols.iter().position(|c| c.name.eq_ignore_ascii_case(&fk.columns[0].value)).ok_or(format!("unknown column {}", fk.columns[0].value))?; fks.push(FkDef { col, parent: obj_name(&fk.foreign_table), on_delete: action(&fk.on_delete)? }); } }
                let pk = pk.ok_or("a PRIMARY KEY column is required")?;
                if let Some(a) = auto { if a != pk { return Err(format!("{} is assigned automatically but is not the primary key; automatic values are for the INT PRIMARY KEY", cols[a].name)); } if cols[pk].ty != ColType::Int { return Err(format!("{}: automatic ids are integers; for a UUID key use DEFAULT uuid()", cols[pk].name)); } }
                let mut schema = Schema::new(&obj_name(&ct.name), cols, pk, unique)?; schema.fks = fks; schema.auto = auto.is_some();
                Ok(Plan::CreateTable(schema))
            }
            Statement::Insert(ins) => {
                let name = match &ins.table { TableObject::TableName(n) => obj_name(n), other => return Err(format!("unsupported insert target {other}")) };
                let table = self.db.table(&name).ok_or(format!("no such table {name}"))?;
                let q = ins.source.as_ref().ok_or("INSERT needs VALUES")?;
                let vrows: Vec<&Vec<ast::Expr>> = match &*q.body { SetExpr::Values(v) => v.rows.iter().map(|r| &r.content).collect(), _ => return Err("only INSERT ... VALUES is supported".into()) };
                if vrows.is_empty() { return Err("empty VALUES".into()); }
                let schema = &table.schema; let ncols = schema.columns.len();
                // a column left out, or given as DEFAULT: the automatic key, a generator, the default, or NULL
                let fill = |i: usize| -> Result<Expr, String> {
                    let c = &schema.columns[i];
                    if i == schema.pk && schema.auto { return Ok(Expr::AutoId); }
                    if let Some(g) = &c.generated { return Ok(Expr::Gen(g.clone())); }
                    if c.default != Value::Null { return Ok(Expr::Lit(c.default.clone())); }
                    if c.nullable { return Ok(Expr::Lit(Value::Null)); }
                    Err(format!("column {} has no value and no default", c.name))
                };
                let targets: Vec<usize> = if ins.columns.is_empty() { (0..ncols).collect() } else { ins.columns.iter().map(|c| { let n = obj_name(c); schema.col(&n).ok_or(format!("no such column {n}")) }).collect::<Result<_, _>>()? };
                let mut rows = Vec::with_capacity(vrows.len()); let nrows = vrows.len();
                for (ri, row) in vrows.into_iter().enumerate() {
                    if row.len() != targets.len() { return Err(values_count_error(schema, &name, if ins.columns.is_empty() { None } else { Some(&targets) }, row.len(), ri, nrows)); }
                    let mut exprs: Vec<Option<Expr>> = vec![None; ncols];
                    for (&i, e) in targets.iter().zip(row) {
                        if format!("{e}").eq_ignore_ascii_case("DEFAULT") { continue; }
                        let x = self.expr(e)?;
                        exprs[i] = Some(match (&x, i == schema.pk) { (Expr::Lit(Value::Null), true) if schema.auto => Expr::AutoId, (Expr::Lit(Value::Null), true) if schema.columns[i].generated.is_some() => Expr::Gen(schema.columns[i].generated.clone().unwrap()), _ => x });
                    }
                    rows.push(exprs.into_iter().enumerate().map(|(i, e)| match e { Some(e) => Ok(e), None => fill(i) }).collect::<Result<Vec<_>, _>>()?);
                }
                let returning = match &ins.returning {
                    None => None,
                    Some(items) => { let mut cols = vec![]; for it in items { match it {
                        SelectItem::Wildcard(_) => cols.extend(0..ncols),
                        SelectItem::UnnamedExpr(e) => { let n = match e { ast::Expr::Identifier(id) => id.value.clone(), ast::Expr::CompoundIdentifier(ids) => ids.last().map(|i| i.value.clone()).unwrap_or_default(), other => return Err(format!("RETURNING supports column names and * (got {other})")) }; cols.push(schema.col(&n).ok_or(format!("no such column {n}"))?); }
                        other => return Err(format!("RETURNING supports column names and * (got {other})")),
                    } } Some(cols) }
                };
                Ok(Plan::Insert { table, rows, returning })
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
                        ast::AlterTableOperation::AddColumn { column_def: c, .. } => { let mut nullable = true; let mut default = Value::Null; let mut generated = None; let ty = coltype_of(&c.data_type)?; for o in &c.options { match &o.option { ast::ColumnOption::NotNull => nullable = false, ast::ColumnOption::Default(e) => { (default, generated) = default_spec(e, &c.name.value, &ty)?; } ast::ColumnOption::Null => nullable = true, other => return Err(format!("unsupported column option {other}")) } } if !nullable && default == Value::Null && generated.is_none() { return Err(format!("column {} is NOT NULL and needs a DEFAULT for existing rows", c.name.value)); } AlterOp::Add { col: Column { name: c.name.value.clone(), ty, nullable, default: default.clone(), generated }, default } }
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
                        let kind = |t: &ColType| match t { ColType::Int | ColType::Timestamp => Some(0), ColType::Uuid => Some(1), _ => None };
                        let (ki, ko) = (kind(&srcs[s].table.schema.columns[c].ty), kind(&self.outer[os].table.schema.columns[oc].ty));
                        if ki.is_none() || ki != ko { return Err(format!("unsupported correlation {e}: both columns must be INT or TIMESTAMP, or both UUID")); }
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
        let mut proj = vec![]; let mut names: Vec<String> = vec![];
        for item in sel.projection.iter().filter(|_| !exists) {
            match item {
                SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                    names.push(match item { SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(), _ => match e { ast::Expr::Identifier(id) => id.value.clone(), ast::Expr::CompoundIdentifier(ids) => ids.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join("."), other => other.to_string() } });
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
                for &c in &step_cols[k] { schema.columns.push(Column { name: format!("{}.{}", srcs[bs].alias, bsch.columns[c].name), ty: bsch.columns[c].ty.clone(), nullable: bsch.columns[c].nullable, default: Value::Null, generated: None }); schema.offsets.push(off); off += bsch.columns[c].ty.width(); }
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
            finish_names(&mut names, &proj, &schema); return Ok(Plan::Select(SelectPlan { table: srcs[probe].table.clone(), schema, access: Access::Scan, preds: vec![], proj, names, group_by, order: order_by, limit, join: Some(join), corr, subs }));
        }
        let mut preds = std::mem::take(&mut side_preds[0]);
        let access = access_path(&srcs[0].table, &mut preds);
        self.cur = saved_cur;
        finish_names(&mut names, &proj, &schema);
        Ok(Plan::Select(SelectPlan { table: srcs[0].table.clone(), schema, access, preds, proj, names, group_by, order: order_by, limit, join: None, corr, subs }))
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
    if let (Some(a), Some(b), false) = (lo, hi, schema.key_is_uuid()) {
        let (pa, pb) = (preds[a].clone(), preds[b].clone());
        let mut rm = vec![a, b]; rm.sort_unstable(); for i in rm.into_iter().rev() { preds.remove(i); }
        return Access::Range { lo: pa.rhs, lo_excl: pa.cmp == Cmp::Gt, hi: pb.rhs, hi_excl: pb.cmp == Cmp::Lt };
    }
    Access::Scan
}

// ---------- execution ----------

#[derive(Clone, Copy, Default)] struct AggState { n: u64, sum_f: f64, sum_i: i64, min: f64, max: f64, seen: bool }
/// A set of integer keys: bitmap when the range is reasonable, hash map otherwise.
pub enum KeySet { Bits { min: i64, max: i64, bits: Vec<u64> }, Hash(FastMap<()>), Wide(crate::util::WideSet), Empty }
impl KeySet {
    /// A set of UUID keys (see `key_from_uuid`).
    pub fn build_wide(keys: &[Key]) -> KeySet { if keys.is_empty() { KeySet::Empty } else { KeySet::Wide(keys.iter().copied().collect()) } }
    #[inline] pub fn contains_wide(&self, k: Key) -> bool { match self { KeySet::Wide(s) => s.contains(&k), _ => false } }
    pub fn build(keys: &[i64]) -> KeySet {
        if keys.is_empty() { return KeySet::Empty; }
        let (min, max) = keys.iter().fold((i64::MAX, i64::MIN), |(a, b), &k| (a.min(k), b.max(k)));
        if (max - min) as u64 + 1 <= 128_000_000 { let mut bits = vec![0u64; ((max - min) as usize >> 6) + 1]; for &k in keys { let d = (k - min) as usize; bits[d >> 6] |= 1 << (d & 63); } KeySet::Bits { min, max, bits } }
        else { let mut m = FastMap::default(); m.reserve(keys.len()); for &k in keys { m.insert(k, ()); } KeySet::Hash(m) }
    }
    #[inline] pub fn contains(&self, k: i64) -> bool { match self { KeySet::Bits { min, max, bits } => k >= *min && k <= *max && { let d = (k - min) as usize; bits[d >> 6] >> (d & 63) & 1 == 1 }, KeySet::Hash(m) => m.contains_key(&k), KeySet::Wide(_) | KeySet::Empty => false } }
}
/// A UUID value (text or 16 bytes) as a key; None for anything else.
fn uuid_key(v: &Value) -> Option<Key> { match v { Value::Text(s) => parse_uuid(s).map(|b| key_from_uuid(&b)), Value::Bytes(b) if b.len() == 16 => Some(key_from_uuid(b)), _ => None } }
/// Correlated scalar results by the outer row's key: INT/TIMESTAMP keys, or UUID keys.
pub enum CorrMap { N(FastMap<f64>), W(crate::util::WideMap<f64>) }
#[inline] fn mix_pair(a: i64, b: i64) -> i64 { ((a as u64).wrapping_mul(0x9E3779B97F4A7C15) ^ (b as u64).rotate_left(32).wrapping_mul(0xC2B2AE3D27D4EB4F)) as i64 }
pub struct CPred { off: usize, kind: u8, cmp: Cmp, i: i64, f: f64, b: Vec<u8>, nb: usize, nm: u8, set: Option<Arc<KeySet>>, pair: Option<(usize, usize, u8)>, corr_map: Option<Arc<CorrMap>>, ckind: u8 }
impl CPred {
    #[inline] fn ok<T: PartialOrd>(cmp: Cmp, a: T, b: T) -> bool { match cmp { Cmp::Eq => a == b, Cmp::Ne => a != b, Cmp::Lt => a < b, Cmp::Le => a <= b, Cmp::Gt => a > b, Cmp::Ge => a >= b, _ => false } }
    #[inline(always)] pub fn eval(&self, row: &[u8]) -> bool {
        let null = row[self.nb] & self.nm != 0;
        match self.cmp {
            Cmp::IsNull => return null, Cmp::IsNotNull => return !null,
            Cmp::In | Cmp::NotIn => {
                if null { return false; }
                let set = self.set.as_ref().unwrap();
                let hit = if self.kind == 4 { set.contains_wide(key_from_uuid(&row[self.off..self.off + 16])) } else {
                    let mut k = read_i64(row, self.off);
                    if let Some((po, pnb, pnm)) = self.pair { if row[pnb] & pnm != 0 { return false; } k = mix_pair(k, read_i64(row, po)); }
                    set.contains(k)
                };
                return if self.cmp == Cmp::In { hit } else { !hit };
            }
            _ => if null { return false; }
        }
        if self.kind == 20 {
            // correlated scalar: compare this column with the inner aggregate for this row's key
            let (co, cnb, cnm) = self.pair.unwrap();
            if row[cnb] & cnm != 0 { return false; }
            let v = match self.corr_map.as_deref().unwrap() { CorrMap::N(m) => m.get(&read_i64(row, co)), CorrMap::W(m) => m.get(&key_from_uuid(&row[co..co + 16])) };
            let v = match v { Some(v) => *v, None => return false };
            let a = if self.ckind == 1 { read_f64(row, self.off) } else { read_i64(row, self.off) as f64 };
            return Self::ok(self.cmp, a, v);
        }
        match self.kind { 0 => Self::ok(self.cmp, read_i64(row, self.off), self.i), 1 => Self::ok(self.cmp, read_f64(row, self.off), self.f), 2 => Self::ok(self.cmp, row[self.off] as i64, self.i), 4 => Self::ok(self.cmp, &row[self.off..self.off + 16], &self.b[..]), 9 => false, _ => Self::ok(self.cmp, read_bytes(row, self.off), &self.b[..]) }
    }
}

/// Consumes rows that passed the predicates; produces projected rows, aggregates, groups, or top-k.
const DENSE_GROUPS: usize = 65_536;
const LAZY_CAP: usize = 200_000;
enum SubVal<'a> { Const(Value), Map { map: FastMap<Value>, off: usize, nb: usize, nm: u8, dflt: Value }, WMap { map: crate::util::WideMap<Value>, off: usize, nb: usize, nm: u8, dflt: Value }, Lazy { f: Box<dyn Fn(i64) -> Value + 'a>, full: Box<dyn Fn() -> FastMap<Value> + 'a>, dflt: Value, off: usize, nb: usize, nm: u8, cache: std::cell::RefCell<FastMap<Value>>, fallback: std::cell::RefCell<Option<FastMap<Value>>> } }
impl<'a> SubVal<'a> {
    fn get(&self, row: &[u8]) -> Value {
        match self {
            SubVal::Const(v) => v.clone(),
            SubVal::Map { map, off, nb, nm, dflt } => if row[*nb] & nm != 0 { Value::Null } else { map.get(&read_i64(row, *off)).cloned().unwrap_or_else(|| dflt.clone()) },
            SubVal::WMap { map, off, nb, nm, dflt } => if row[*nb] & nm != 0 { Value::Null } else { map.get(&key_from_uuid(&row[*off..*off + 16])).cloned().unwrap_or_else(|| dflt.clone()) },
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
/// An ORDER BY key for a numeric column as one unsigned integer: NULL below every value, integers and floats in
/// their own order (floats in IEEE total order), DESC by inverting the bits. One compare per row on the hot path.
#[inline(always)] fn num_sort_key(kind: u8, row: &[u8], off: usize, null: bool, desc: bool) -> u128 {
    let k: u128 = if null { 0 } else { (1u128 << 64) | match kind {
        0 => (read_i64(row, off) as u64 ^ (1 << 63)) as u128,
        1 => { let b = read_f64(row, off).to_bits(); (if b >> 63 == 1 { !b } else { b | (1 << 63) }) as u128 }
        _ => (row[off] != 0) as u128 } };
    if desc { !k } else { k }
}
/// Order of decoded values of one column type (group keys, sorted results); NULL first.
fn value_order(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    match (a, b) { (Value::Null, Value::Null) => Equal, (Value::Null, _) => Less, (_, Value::Null) => Greater, (Value::Int(x), Value::Int(y)) => x.cmp(y), (Value::Float(x), Value::Float(y)) => x.total_cmp(y), (Value::Bool(x), Value::Bool(y)) => x.cmp(y), (Value::Text(x), Value::Text(y)) => x.cmp(y), (Value::Bytes(x), Value::Bytes(y)) => x.cmp(y), _ => Equal }
}
/// The bytes that identify a non-integer GROUP BY value (text by its content, not its padded cell; -0.0 as 0.0).
#[inline] fn group_key_into(schema: &Schema, row: &[u8], c: usize, buf: &mut Vec<u8>) {
    let off = schema.offsets[c];
    match &schema.columns[c].ty {
        ColType::Uuid => buf.extend_from_slice(&row[off..off + 16]),
        ColType::Bool => buf.push((row[off] != 0) as u8),
        ColType::Float => { let f = read_f64(row, off); buf.extend_from_slice(&(if f == 0.0 { 0.0f64 } else { f }).to_bits().to_le_bytes()); }
        ColType::Text(_) | ColType::Bytes(_) => buf.extend_from_slice(read_bytes(row, off)),
        ColType::Int | ColType::Timestamp => buf.extend_from_slice(&read_i64(row, off).to_le_bytes()),
    }
}
struct Sink<'a> { schema: &'a Schema, proj: &'a [Proj], subs: Vec<SubVal<'a>>, aggs: Vec<&'a Agg>, has_agg: bool, group_by: Option<usize>, group_off: Option<usize>, order: Option<(usize, bool)>, limit: usize, single: Vec<AggState>, groups: FastMap<u32>, gstates: Vec<AggState>, dense: Vec<AggState>, dense_seen: Vec<bool>, null_group: Option<Vec<AggState>>, topk: Vec<(u128, Vec<Value>)>, out: Vec<Vec<Value>>,
    /// ORDER BY: the column, its offset, 0 INT/TIMESTAMP, 1 DOUBLE, 2 BOOL, 3 bytes (TEXT, BYTES, UUID: `topk_b`, NULL as None)
    sort_col: usize, sort_off: usize, sort_kind: u8, topk_b: Vec<(Option<Vec<u8>>, Vec<Value>)>,
    /// GROUP BY a column that is not INT/TIMESTAMP: groups keyed by the value's bytes, `bvals[i]` is group i's value
    wide_group: bool, bgroups: HashMap<Box<[u8]>, u32>, bvals: Vec<Value>, kbuf: Vec<u8> }
impl<'a> Sink<'a> {
    fn new(schema: &'a Schema, proj: &'a [Proj], group_by: Option<usize>, order: Option<(usize, bool)>, limit: Option<usize>, subs: Vec<SubVal<'a>>) -> Sink<'a> {
        let aggs: Vec<&Agg> = proj.iter().filter_map(|p| if let Proj::Agg(a) = p { Some(a) } else { None }).collect();
        let sort_col = match order.map(|(oi, _)| &proj[oi]) { Some(Proj::Col(c)) => *c, _ => 0 };
        Sink { schema, proj, subs, has_agg: !aggs.is_empty(), single: vec![AggState::default(); aggs.len()], aggs, group_by, group_off: group_by.filter(|&g| matches!(schema.columns[g].ty, ColType::Int | ColType::Timestamp)).map(|g| schema.offsets[g]), order, limit: limit.unwrap_or(usize::MAX), groups: FastMap::default(), gstates: vec![], dense: vec![], dense_seen: vec![], null_group: None, topk: vec![], out: vec![],
            wide_group: group_by.is_some_and(|g| !matches!(schema.columns[g].ty, ColType::Int | ColType::Timestamp)), bgroups: HashMap::new(), bvals: vec![], kbuf: vec![],
            sort_col, sort_off: schema.offsets.get(sort_col).copied().unwrap_or(0), sort_kind: match schema.columns.get(sort_col).map(|c| &c.ty) { Some(ColType::Float) => 1, Some(ColType::Bool) => 2, Some(ColType::Text(_) | ColType::Bytes(_) | ColType::Uuid) => 3, _ => 0 }, topk_b: vec![] }
    }
    /// The group index for a row grouped by a column that is not INT/TIMESTAMP (kept out of line: the integer path is hot).
    #[inline(never)] fn wide_index(&mut self, row: &[u8], n: usize) -> usize {
        let g = self.group_by.unwrap();
        self.kbuf.clear(); group_key_into(self.schema, row, g, &mut self.kbuf);
        match self.bgroups.get(&self.kbuf[..]) { Some(&i) => i as usize, None => {
            let i = self.bvals.len(); self.bgroups.insert(self.kbuf.clone().into_boxed_slice(), i as u32);
            self.bvals.push(decode_col(self.schema, row, g)); self.gstates.extend(std::iter::repeat(AggState::default()).take(n)); i } }
    }
    /// ORDER BY a TEXT, BYTES or UUID column (out of line; allocates only for rows that enter the top k).
    #[inline(never)] fn push_sorted_bytes(&mut self, row: &[u8], desc: bool) {
        let (c, off) = (self.sort_col, self.sort_off);
        let key: Option<&[u8]> = if is_null(row, c) { None } else if self.sort_kind == 3 && self.schema.columns[c].ty == ColType::Uuid { Some(&row[off..off + 16]) } else { Some(read_bytes(row, off)) };
        let ord = |a: Option<&[u8]>, b: Option<&[u8]>| { let o = a.cmp(&b); if desc { o.reverse() } else { o } };
        if self.topk_b.len() >= self.limit && ord(key, self.topk_b[self.limit - 1].0.as_deref()) != std::cmp::Ordering::Less { return; }
        let vals: Vec<Value> = self.proj.iter().map(|p| match p { Proj::Col(c) => decode_col(self.schema, row, *c), Proj::Agg(_) => Value::Null, Proj::Sub(i) => self.subs[*i].get(row) }).collect();
        let pos = self.topk_b.partition_point(|x| ord(x.0.as_deref(), key) != std::cmp::Ordering::Greater); self.topk_b.insert(pos, (key.map(|k| k.to_vec()), vals));
        if self.topk_b.len() > self.limit { self.topk_b.pop(); }
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
            } else if self.wide_group {
                if is_null(row, self.group_by.unwrap()) { self.null_group.get_or_insert_with(|| vec![AggState::default(); n]) }
                else { let i = self.wide_index(row, n); &mut self.gstates[i * n..(i + 1) * n] }
            } else { &mut self.single };
            for (k, a) in self.aggs.iter().enumerate() {
                let st = &mut st[k];
                match a { Agg::CountStar => st.n += 1, Agg::CountCol(c) => if !is_null(row, *c) { st.n += 1 }, Agg::Sum(c) | Agg::Avg(c) | Agg::Min(c) | Agg::Max(c) => {
                    if is_null(row, *c) { continue; }
                    let off = self.schema.offsets[*c];
                    let (fi, ff) = match self.schema.columns[*c].ty { ColType::Int | ColType::Timestamp => { let i = read_i64(row, off); (i, i as f64) }, ColType::Float => { let f = read_f64(row, off); (0, f) }, _ => (0, 0.0) };
                    st.n += 1; st.sum_i += fi; st.sum_f += ff; if !st.seen || ff < st.min { st.min = ff; } if !st.seen || ff > st.max { st.max = ff; } st.seen = true; } }
            }
            return true;
        }
        if let Some((_, desc)) = self.order {
            // compare on raw bytes first; decode only survivors
            if self.sort_kind == 3 { self.push_sorted_bytes(row, desc); return true; }
            let key = num_sort_key(self.sort_kind, row, self.sort_off, is_null(row, self.sort_col), desc);
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
        } else if self.wide_group {
            if is_null(row, self.group_by.unwrap()) { self.null_group.get_or_insert_with(|| vec![AggState::default(); na]) }
            else { let i = self.wide_index(row, na); &mut self.gstates[i * na..(i + 1) * na] }
        } else { &mut self.single };
        for x in st.iter_mut() { x.n += n; }
        true
    }
    fn finish(mut self) -> Vec<Vec<Value>> {
        if self.has_agg {
            let schema = self.schema; let proj = self.proj; let group_by = self.group_by;
            let emit = |key: Option<Value>, st: &[AggState]| -> Vec<Value> {
                let mut k = 0;
                proj.iter().map(|p| match p {
                    Proj::Col(c) => if Some(*c) == group_by { key.clone().unwrap_or(Value::Null) } else { Value::Null },
                    Proj::Sub(_) => Value::Null,
                    Proj::Agg(a) => { let x = st[k]; k += 1; match a {
                        Agg::CountStar | Agg::CountCol(_) => Value::Int(x.n as i64),
                        Agg::Sum(c) => if x.n == 0 { Value::Null } else if matches!(schema.columns[*c].ty, ColType::Int | ColType::Timestamp) { Value::Int(x.sum_i) } else { Value::Float(x.sum_f) },
                        Agg::Avg(_) => if x.n == 0 { Value::Null } else { Value::Float(x.sum_f / x.n as f64) },
                        Agg::Min(c) => if x.n == 0 { Value::Null } else if matches!(schema.columns[*c].ty, ColType::Int | ColType::Timestamp) { Value::Int(x.min as i64) } else { Value::Float(x.min) },
                        Agg::Max(c) => if x.n == 0 { Value::Null } else if matches!(schema.columns[*c].ty, ColType::Int | ColType::Timestamp) { Value::Int(x.max as i64) } else { Value::Float(x.max) } } }
                }).collect()
            };
            if self.wide_group {
                if let Some(ng) = &self.null_group { let r = emit(None, ng); self.out.push(r); }
                let n = self.aggs.len();
                let mut idx: Vec<usize> = (0..self.bvals.len()).collect();
                idx.sort_unstable_by(|&a, &b| value_order(&self.bvals[a], &self.bvals[b]));
                for i in idx { let r = emit(Some(self.bvals[i].clone()), &self.gstates[i * n..(i + 1) * n]); self.out.push(r); }
            } else if self.group_off.is_some() {
                if let Some(ng) = &self.null_group { let r = emit(None, ng); self.out.push(r); }
                let n = self.aggs.len();
                let mut keys: Vec<i64> = self.groups.keys().copied().collect();
                for (k, seen) in self.dense_seen.iter().enumerate() { if *seen { keys.push(k as i64); } }
                keys.sort_unstable();
                for k in keys { let r = if k >= 0 && (k as usize) < DENSE_GROUPS && !self.dense.is_empty() && self.dense_seen[k as usize] { emit(Some(Value::Int(k)), &self.dense[k as usize * n..(k as usize + 1) * n]) } else { let i = self.groups[&k] as usize; emit(Some(Value::Int(k)), &self.gstates[i * n..(i + 1) * n]) }; self.out.push(r); }
            }
            else { let r = emit(None, &self.single); self.out.push(r); }
            // groups come out in key order; ORDER BY the group column (or DESC) reorders them before LIMIT
            if let Some((oi, desc)) = self.order { self.out.sort_by(|a, b| { let o = value_order(&a[oi], &b[oi]); if desc { o.reverse() } else { o } }); }
            if self.limit != usize::MAX { self.out.truncate(self.limit); }
            return self.out;
        }
        if self.order.is_some() { return if self.sort_kind == 3 { self.topk_b.into_iter().map(|(_, v)| v).collect() } else { self.topk.into_iter().map(|(_, v)| v).collect() }; }
        self.out
    }
}

pub struct Sql { pub db: Arc<Database>, cache: Arc<Mutex<HashMap<String, Arc<Prepared>>>>, pub acl: std::sync::RwLock<Acl>, pub epoch: Arc<std::sync::atomic::AtomicU64>, alters: Mutex<Vec<std::thread::JoinHandle<Result<(), String>>>>, pub live: std::sync::OnceLock<Arc<crate::live::Reactor>>,
    /// The configuration this database was opened with (defaults when opened through the lab constructors).
    pub cfg: crate::config::Config, services: Mutex<Option<Services>>,
    /// `<dir>/.lock`, held while open. Declared last: fields drop in order, so the lock is released only after `db`
    /// (and with it the log threads) has gone.
    dir_lock: Mutex<Option<std::fs::File>> }
/// Background work `open_with` started for this database; stopped by `close` (or when the last handle drops).
#[derive(Default)] pub struct Services { pub uploader: Option<crate::cloud::Uploader>, pub puller: Option<crate::cloud::BucketPuller>, pub follower: Option<Follower>, snapshotter: Option<(Arc<std::sync::atomic::AtomicBool>, std::thread::JoinHandle<()>)>, listeners: Vec<crate::net::Listener>, pub snapshots: Arc<std::sync::atomic::AtomicU64> }
impl Drop for Sql { fn drop(&mut self) { self.close(); } }
impl Sql { /// The reactive layer for this database (started on first use).
    pub fn reactor(self: &Arc<Self>) -> Arc<crate::live::Reactor> { self.live.get_or_init(|| crate::live::Reactor::start(self.clone())).clone() } }
/// One committed change, decoded against the table's schema.
#[derive(Debug, Clone, PartialEq)] pub enum ChangeOp { Insert, Update, Delete }
#[derive(Debug, Clone)] pub struct Change { pub seq: u64, pub table: String, pub op: ChangeOp, /// the row's key: an INT, or a UUID as text
    pub pk: Value, pub row: Option<Vec<Value>> }
/// A change feed: tails the log and decodes records into `Change`s. Records written to a table that no longer owns its
/// name (a shadow during a schema change, a retired table after one) are skipped, so one logical change is one event.
pub struct Feed { cursor: LogCursor, db: Arc<Database>, last: u64, on_ring: bool }
impl Drop for Feed { fn drop(&mut self) { self.db.log.feeds.fetch_sub(1, std::sync::atomic::Ordering::Relaxed); } }
impl Feed {
    pub fn last_seq(&self) -> u64 { self.last }
    /// Raw records since the last poll, flattened (transactions become one entry per write), owner-filtered: (seq, table, op, payload).
    /// Records since the last poll that belong to a table's current owner. Empty means caught up: a batch whose records
    /// were all filtered out (an online ALTER's copy writes, say) does not end the poll, the next batch is read.
    pub fn poll_raw(&mut self) -> Vec<(u64, Arc<Table>, u8, Vec<u8>)> {
        loop {
            let before = self.last;
            let out = self.poll_batch();
            if !out.is_empty() || self.last == before { return out; }
        }
    }
    fn poll_batch(&mut self) -> Vec<(u64, Arc<Table>, u8, Vec<u8>)> {
        let durable_only = self.db.durable;
        let (ring, ok) = self.db.log.recent_after(self.last, durable_only);
        let recs: Vec<(u64, u8, u16, Vec<u8>)> = if ok { self.on_ring = true; ring.into_iter().map(|(a, b, c, p)| (a, b, c, p.to_vec())).collect() }
            else { if self.on_ring { self.cursor = LogCursor::new(&self.db.dir, self.last); self.on_ring = false; } let upto = if durable_only { self.db.log.st.lock().unwrap().durable } else { u64::MAX }; self.cursor.poll_upto(upto) };
        let mut out = vec![];
        let push = |seq: u64, tid: u16, op: u8, payload: &[u8], out: &mut Vec<(u64, Arc<Table>, u8, Vec<u8>)>| { if let Some(t) = self.db.table_by_id(tid) { let (since, until) = (t.owner_since.load(std::sync::atomic::Ordering::Relaxed), t.owner_until.load(std::sync::atomic::Ordering::Relaxed)); if seq >= since && seq < until { out.push((seq, t, op, payload.to_vec())); } } };
        for (seq, op, tid, payload) in recs {
            self.last = self.last.max(seq);
            match op {
                OP_TXN => { let mut p = 0; while p + 7 <= payload.len() { let t2 = u16::from_le_bytes(payload[p..p + 2].try_into().unwrap()); let op2 = payload[p + 2]; let len = u32::from_le_bytes(payload[p + 3..p + 7].try_into().unwrap()) as usize; p += 7; push(seq, t2, op2, &payload[p..p + len], &mut out); p += len; } }
                OP_META => {}
                _ => push(seq, tid, op, &payload, &mut out),
            }
        }
        out
    }
    /// Decoded changes since the last poll (empty if none yet). In-process: served from the log's memory ring the moment a
    /// record is committed (durable mode: once it is durable); the files are read only to catch up after a gap.
    pub fn poll(&mut self) -> Vec<Change> {
        let mut out = vec![];
        for (seq, t, op, payload) in self.poll_raw() { self.decode_owned(seq, &t, op, &payload, &mut out); }
        out
    }
    fn decode_owned(&self, seq: u64, t: &Arc<Table>, op: u8, payload: &[u8], out: &mut Vec<Change>) {
        match op {
            OP_DELETE => { let pk = t.schema.key_value(if payload.len() == t.schema.key_width() { t.schema.key_from_bytes(payload) } else { t.pk_of(payload) }); let row = if payload.len() == t.schema.row_size { Some((0..t.schema.columns.len()).map(|i| decode_col(&t.schema, payload, i)).collect()) } else { None }; out.push(Change { seq, table: t.schema.name.clone(), op: ChangeOp::Delete, pk, row }); }
            OP_INSERT | OP_UPDATE => { if payload.len() != t.schema.row_size { return; } let row: Vec<Value> = (0..t.schema.columns.len()).map(|i| decode_col(&t.schema, payload, i)).collect(); out.push(Change { seq, table: t.schema.name.clone(), op: if op == OP_INSERT { ChangeOp::Insert } else { ChangeOp::Update }, pk: t.schema.key_value(t.pk_of(payload)), row: Some(row) }); }
            _ => {}
        }
    }
}
/// Handle on a follower's replication thread.
pub struct Follower { pub applied: Arc<std::sync::atomic::AtomicU64>, stop: Arc<std::sync::atomic::AtomicBool>, thread: Option<std::thread::JoinHandle<()>> }
impl Follower { pub fn applied(&self) -> u64 { self.applied.load(std::sync::atomic::Ordering::Acquire) } }
impl Drop for Follower { fn drop(&mut self) { self.stop.store(true, std::sync::atomic::Ordering::Relaxed); if let Some(h) = self.thread.take() { h.join().ok(); } } }
impl Sql {
    pub fn new(db: Arc<Database>) -> Sql { Sql { db, cache: Arc::new(Mutex::new(HashMap::new())), acl: std::sync::RwLock::new(Acl::default()), epoch: Arc::new(std::sync::atomic::AtomicU64::new(0)), alters: Mutex::new(vec![]), live: std::sync::OnceLock::new(), cfg: crate::config::Config::default(), services: Mutex::new(None), dir_lock: Mutex::new(None) } }
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
        schema.auto = old.auto;
        let (old_s, new_s) = (old.clone(), schema.clone()); let srcs: Vec<(Option<usize>, Value)> = cols.iter().map(|(_, s, d)| (*s, d.clone())).collect();
        let convert = Arc::new(move |row: &[u8]| -> Result<Vec<u8>, String> {
            let vals: Vec<Value> = srcs.iter().enumerate().map(|(i, (src, dflt))| match src { Some(j) => { let v = decode_col(&old_s, row, *j); if v == Value::Null && !new_s.columns[i].nullable { Value::Null } else { v } } None => match &new_s.columns[i].generated { Some(g) => crate::table::gen_value(g), None => dflt.clone() } }).collect();
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
        // The copy runs on its own thread so readers and writers keep going; the statement waits for it, so a change
        // that cannot be applied (a value that does not convert) is an error for the caller instead of a silent
        // ABORT in the log.
        self.spawn_copy(table.to_string()).join().map_err(|_| "alter thread panicked".to_string())?
    }
    /// Resume an interrupted schema change in the background (at open).
    fn resume_copy(&self, table: String) { let h = self.spawn_copy(table); self.alters.lock().unwrap().push(h); }
    fn spawn_copy(&self, table: String) -> std::thread::JoinHandle<Result<(), String>> {
        let (db, cache, epoch) = (self.db.clone(), self.cache.clone(), self.epoch.clone());
        let h = std::thread::spawn(move || {
            let meta = |text: &str| { let seq = db.log.append_meta(text); if db.durable { db.log.wait_durable(seq); } };
            let old = db.table(&table).ok_or("table vanished")?;
            let sh = old.shadow.read().unwrap().clone().ok_or("no shadow")?;
            let stop = std::sync::atomic::AtomicBool::new(false);
            match old.copy_into_shadow(&sh, &stop) {
                Ok(_) => { let old = db.alter_finish(&table)?; meta(&format!("ALTER TABLE {table} DONE")); cache.lock().unwrap().clear(); epoch.fetch_add(1, std::sync::atomic::Ordering::Release); std::thread::spawn(move || { std::thread::sleep(std::time::Duration::from_secs(1)); old.clear(); }); Ok(()) }  // grace period, off the caller's path: statements planned before the swap drain on the old table, still mirrored
                Err(e) => { db.alter_abort(&table)?; meta(&format!("ALTER TABLE {table} ABORT")); Err(format!("ALTER TABLE {table} was not applied: {e}")) }
            }
        });
        h
    }
    /// Wait for every background schema change; returns the first error, if any.
    pub fn wait_alters(&self) -> Result<(), String> { let hs: Vec<_> = self.alters.lock().unwrap().drain(..).collect(); let mut r = Ok(()); for h in hs { if let Err(e) = h.join().map_err(|_| "alter thread panicked".to_string())? { r = Err(e); } } r }
    /// Create a fresh database directory and start it.
    /// Wipe `dir` and start an empty database there. For tests and experiments; applications use `open_with`.
    pub fn recreate(dir: &std::path::Path, durable: bool) -> Sql { let _ = std::fs::remove_dir_all(dir); let db = Arc::new(Database::open(dir, durable)); db.start(); Sql::new(db) }
    /// Create with log threads that leave after `idle` of silence (tenant hosting).
    pub fn recreate_idle(dir: &std::path::Path, durable: bool, idle: std::time::Duration) -> Sql { let _ = std::fs::remove_dir_all(dir); let db = Arc::new(Database::open(dir, durable)); db.start_idle(idle); Sql::new(db) }
    pub fn open_idle(dir: &std::path::Path, durable: bool, idle: std::time::Duration) -> Result<(Sql, RecStats), String> { let db = Arc::new(Database::open(dir, durable)); let sql = Sql::new(db.clone()); let st = db.recover(1, &mut |text| sql.apply_meta(text))?; db.start_idle(idle); Ok((sql, st)) }
    /// Open an existing database: replay the snapshot catalog + rows, then the log tail, then start the committer.
    pub fn open(dir: &std::path::Path, durable: bool, threads: usize) -> Result<(Sql, RecStats), String> {
        let db = Arc::new(Database::open(dir, durable));
        let sql = Sql::new(db.clone());
        let st = db.recover(threads, &mut |text| sql.apply_meta(text))?;
        db.start();
        Ok((sql, st))
    }
    /// Open (or create) `dir` with a full configuration: durability mode, backup to a bucket, follower mode, snapshot
    /// policy, listeners, reactor settings. Starts the background services and returns the shared handle; `close`
    /// (or dropping the last handle) stops them. Never wipes an existing directory.
    pub fn open_with(dir: &std::path::Path, cfg: crate::config::Config) -> Result<Arc<Sql>, String> {
        use crate::config::{Durability, Follow};
        if let Some(f) = &cfg.follow {
            let mut sv = Services::default();
            let (src, poll) = match f {
                Follow::Dir { dir: src, poll } => { if crate::table::check_dir(src)? == crate::table::DirState::Empty { return Err(format!("follow.dir {} holds no sqweezdb database", src.display())); } (src.clone(), *poll) }
                Follow::Bucket { source, poll } => { crate::table::check_dir(dir)?; sv.puller = Some(crate::cloud::start_puller(source.cloud(), dir.to_path_buf(), *poll)); (dir.to_path_buf(), *poll) }
            };
            let lock = if matches!(f, Follow::Bucket { .. }) { Some(lock_dir(dir)?) } else { None };
            let (sql, follower) = Sql::follow(&src, poll)?;
            *sql.dir_lock.lock().unwrap() = lock;
            sv.follower = Some(follower);
            if let Some(p) = &cfg.listen.socket { sv.listeners.push(crate::net::Server::start(sql.clone(), p).map_err(|e| e.to_string())?); }
            if let Some(a) = &cfg.listen.tcp { sv.listeners.push(crate::net::Server::start_tcp(sql.clone(), a, cfg.listen.token.as_deref().unwrap_or("")).map_err(|e| e.to_string())?); }
            *sql.services.lock().unwrap() = Some(sv);
            return Ok(sql);
        }
        // decide before writing anything: a file, a foreign non-empty directory or a newer format is refused
        let existing = crate::table::check_dir(dir)? != crate::table::DirState::Empty;
        if !existing && !cfg.create { return Err(format!("no database in {} and create is off", dir.display())); }
        let lock = lock_dir(dir)?;   // one owner per directory, across processes and handles
        if !existing {
            if let Some(b) = &cfg.backup { if b.bootstrap { bootstrap_from_bucket(dir, b)?; } }
        }
        let db = Arc::new(Database::open(dir, cfg.durable == Durability::Fsync));
        crate::table::write_meta_if_missing(dir).map_err(|e| format!("{}: cannot write {}: {e}", dir.display(), crate::table::META_FILE))?;
        let mut sql = Sql::new(db.clone()); sql.cfg = cfg.clone();
        db.recover(cfg.recover_threads, &mut |text| sql.apply_meta(text))?;
        match cfg.log.threads_idle_after { Some(d) => db.start_idle(d), None => db.start() }
        let sql = Arc::new(sql);
        for (name, _) in sql.db.tables_with_shadow() { sql.resume_copy(name); }   // resume an interrupted ALTER
        *sql.dir_lock.lock().unwrap() = Some(lock);
        let mut sv = Services::default();
        if let Some(b) = &cfg.backup {
            let up = crate::cloud::start_uploader(b.cloud(), dir.to_path_buf(), db.log.clone());
            if cfg.durable == Durability::Cloud { db.log.set_cloud_wait(true, Some(up.kick.clone())); }
            sv.uploader = Some(up);
        }
        if let Some(p) = &cfg.listen.socket { sv.listeners.push(crate::net::Server::start(sql.clone(), p).map_err(|e| e.to_string())?); }
        if let Some(a) = &cfg.listen.tcp { sv.listeners.push(crate::net::Server::start_tcp(sql.clone(), a, cfg.listen.token.as_deref().unwrap_or("")).map_err(|e| e.to_string())?); }
        if cfg.snapshot.log_bytes.is_some() || cfg.snapshot.idle_after.is_some() { sv.snapshotter = Some(spawn_snapshotter(&sql.db, cfg.snapshot.clone(), sv.snapshots.clone())); }
        if let Some(a) = &cfg.listen.http { sv.listeners.push(start_http(&sql, a, cfg.listen.token.as_deref().unwrap_or(""))?); }
        *sql.services.lock().unwrap() = Some(sv);
        Ok(sql)
    }
    /// Run one statement with its output column names: the shape tools want (the console, the shell).
    pub fn query(&self, sess: &mut Session, text: &str, params: &[Value]) -> Result<(Vec<(String, Option<ColType>)>, Vec<Vec<Value>>), String> {
        if is_admin_stmt(text) { return self.exec_s(sess, text, params).map(|r| (vec![], r)); }
        let p = self.prepare(text)?; let rows = self.execute_s(sess, &p, params)?; Ok((self.columns(&p), rows))
    }
    /// Rows in all tables right now (the engine's own count, no scan).
    pub fn snapshot_rows(&self) -> u64 { let mut sess = Session::default(); self.db.table_names().iter().map(|n| self.exec_s(&mut sess, &format!("SELECT count(*) FROM {n}"), &[]).ok().and_then(|r| r.first().and_then(|r| r.first()).and_then(|v| if let Value::Int(i) = v { Some(*i as u64) } else { None })).unwrap_or(0)).sum() }
    /// Upload counters of the configured backup, if any.
    pub fn backup_stats(&self) -> Option<Arc<crate::cloud::CloudStats>> { self.services.lock().unwrap().as_ref().and_then(|s| s.uploader.as_ref().map(|u| u.stats.clone())) }
    /// Snapshots the policy thread has written since open.
    pub fn snapshots_taken(&self) -> u64 { self.services.lock().unwrap().as_ref().map(|s| s.snapshots.load(std::sync::atomic::Ordering::Relaxed)).unwrap_or(0) }
    /// Rows the follower has applied (follower mode), or None.
    pub fn follower_applied(&self) -> Option<u64> { self.services.lock().unwrap().as_ref().and_then(|s| s.follower.as_ref().map(|f| f.applied())) }
    /// Stop everything `open_with` started: listeners (ports freed, open connections end), the snapshot policy, backup,
    /// follower, and the reactor behind subscriptions. Idempotent. The directory lock is released when the last handle
    /// drops and the engine has stopped.
    pub fn close(&self) {
        let sv = self.services.lock().unwrap().take();
        if let Some(mut sv) = sv {
            sv.listeners.clear();
            if let Some((stop, h)) = sv.snapshotter.take() { stop.store(true, std::sync::atomic::Ordering::Relaxed); if h.thread().id() != std::thread::current().id() { h.join().ok(); } }
            self.db.log.set_cloud_wait(false, None);
            sv.uploader.take(); sv.puller.take(); sv.follower.take();
        }
        if let Some(r) = self.live.get() { r.shutdown(); }
    }
    /// Like `open`, and resumes any schema change that was interrupted (its shadow exists, no DONE/ABORT logged).
    pub fn open_resume(dir: &std::path::Path, durable: bool, threads: usize) -> Result<(Sql, RecStats, usize), String> {
        let (sql, st) = Sql::open(dir, durable, threads)?;
        let pending = sql.db.tables_with_shadow(); let n = pending.len();
        for (name, _) in pending { sql.resume_copy(name); }
        Ok((sql, st, n))
    }
    pub fn snapshot(&self) -> SnapStats { self.db.snapshot() }
    /// Block until commit `seq` (e.g. `Session::last_seq`) has been fsynced; works in both durability modes.
    pub fn wait_durable(&self, seq: u64) { self.db.log.wait_durable(seq) }
    /// Output columns of a prepared statement: (name, type when known). Empty for statements that return no rows.
    pub fn columns(&self, p: &Prepared) -> Vec<(String, Option<ColType>)> {
        match &p.plan {
            Plan::Select(s) => s.proj.iter().enumerate().map(|(i, pr)| {
                let name = s.names.get(i).cloned().unwrap_or_else(|| format!("col{i}"));
                let ty = match pr {
                    Proj::Col(v) => s.schema.columns.get(*v).map(|c| c.ty.clone()),
                    Proj::Agg(Agg::Min(c)) | Proj::Agg(Agg::Max(c)) => s.schema.columns.get(*c).map(|c| c.ty.clone()),
                    Proj::Agg(Agg::Sum(c)) => s.schema.columns.get(*c).map(|c| if c.ty == ColType::Float { ColType::Float } else { ColType::Int }),
                    Proj::Agg(Agg::CountStar) | Proj::Agg(Agg::CountCol(_)) => Some(ColType::Int),
                    Proj::Agg(Agg::Avg(_)) => Some(ColType::Float),
                    Proj::Sub(_) => None,
                };
                (name, ty)
            }).collect(),
            Plan::Delete { .. } => vec![("changes".to_string(), Some(ColType::Int))],
            Plan::Insert { table, returning: Some(cols), .. } => cols.iter().map(|&c| (table.schema.columns[c].name.clone(), Some(table.schema.columns[c].ty.clone()))).collect(),
            _ => vec![],
        }
    }
    /// Open as a follower of another database directory: map its snapshot, replay its log, then keep applying new
    /// records as the owner commits them. Never writes to that directory and never starts a committer. Reads are
    /// ordinary SQL; `applied` is the last sequence seen.
    pub fn follow(dir: &std::path::Path, poll: std::time::Duration) -> Result<(Arc<Sql>, Follower), String> {
        let db = Arc::new(Database::open(dir, false));
        let sql = Arc::new(Sql::new(db.clone()));
        let st = db.recover(1, &mut |text| sql.apply_meta(text))?;
        let applied = Arc::new(std::sync::atomic::AtomicU64::new(db.log.st.lock().unwrap().seq)); let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (db2, applied2, stop2, dir2, sql2) = (db.clone(), applied.clone(), stop.clone(), dir.to_path_buf(), Arc::downgrade(&sql));
        let h = std::thread::spawn(move || {
            let mut cur = LogCursor::new(&dir2, applied2.load(std::sync::atomic::Ordering::Relaxed));
            while !stop2.load(std::sync::atomic::Ordering::Relaxed) {
                let recs = cur.poll();
                if recs.is_empty() { std::thread::sleep(poll); continue; }
                let Some(sql2) = sql2.upgrade() else { return };   // the database handle is gone
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
        std::fs::create_dir_all(dest).map_err(|e| e.to_string())?; crate::table::write_meta_if_missing(dest).map_err(|e| e.to_string())?; let mut bytes = 0;
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
        sess.user = Some(user.to_string()); sess.roles = u.roles.clone(); sess.attrs = u.attrs.clone(); sess.assumed = false; Ok(())
    }
    /// `assume` with the identity as JSON: `{"roles": ["customer"], "attrs": {"tenant": 42}}`. Whole numbers become INT,
    /// other numbers DOUBLE, strings TEXT (a UUID as its text), true/false BOOL.
    pub fn assume_json(&self, sess: &mut Session, json: &str) -> Result<(), String> {
        let j: serde_json::Value = serde_json::from_str(json).map_err(|e| format!("identity: {e}"))?;
        let roles: Vec<String> = match j.get("roles") { Some(serde_json::Value::Array(a)) => a.iter().map(|r| r.as_str().map(str::to_string).ok_or("identity: roles must be strings".to_string())).collect::<Result<_, _>>()?, Some(serde_json::Value::String(r)) => vec![r.clone()], None => vec![], _ => return Err("identity: roles must be a list of role names".into()) };
        let mut attrs = HashMap::new();
        match j.get("attrs") { None | Some(serde_json::Value::Null) => {}, Some(serde_json::Value::Object(m)) => for (k, v) in m { attrs.insert(k.clone(), match v {
            serde_json::Value::Null => Value::Null, serde_json::Value::Bool(b) => Value::Bool(*b), serde_json::Value::String(s) => Value::Text(s.clone()),
            serde_json::Value::Number(n) => match n.as_i64() { Some(i) => Value::Int(i), None => Value::Float(n.as_f64().unwrap_or(f64::NAN)) },
            _ => return Err(format!("session attribute {k} must be a number, text or true/false")) }); }, _ => return Err("identity: attrs must be an object".into()) }
        self.assume(sess, &roles, attrs)
    }
    /// Act as an identity the application vouches for: its own login decided who is asking, so it passes the roles and
    /// the session attributes the row policies read (`SESSION.tenant`), and nothing about the user is stored here.
    /// Only code holding the database can do this; the network listeners log in by user name only.
    pub fn assume(&self, sess: &mut Session, roles: &[String], attrs: HashMap<String, Value>) -> Result<(), String> {
        if sess.txn.is_some() { return Err("cannot change identity inside a transaction".into()); }
        if roles.is_empty() { return Err("an identity needs at least one role".into()); }
        { let acl = self.acl.read().unwrap(); if let Some(r) = roles.iter().find(|r| !acl.roles.contains(*r)) { return Err(format!("no such role {r}")); } }
        if let Some(k) = attrs.iter().find(|(_, v)| **v == Value::Null).map(|(k, _)| k) { return Err(format!("session attribute {k} has no value")); }
        sess.user = Some(roles.join(", ")); sess.roles = roles.to_vec(); sess.attrs = attrs; sess.assumed = true; Ok(())
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
    pub fn guard(&self, sess: &Session, table: &Table, cmd: u8) -> Result<Vec<CPred>, String> {
        let user = match &sess.user { None => return Ok(vec![]), Some(u) => u };
        let acl = self.acl.read().unwrap();
        let tname = table.schema.name.to_lowercase();
        let allowed = sess.roles.iter().any(|r| acl.grants.get(&(r.clone(), tname.clone())).map(|m| m & cmd != 0).unwrap_or(false));
        let who = if !sess.assumed { format!("user {user}") } else if sess.roles.len() == 1 { format!("role {user}") } else { format!("roles {user}") };
        if !allowed { return Err(format!("permission denied: {who} lacks {} on {}", match cmd { P_SELECT => "SELECT", P_INSERT => "INSERT", P_DELETE => "DELETE", _ => "UPDATE" }, table.schema.name)); }
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
    pub fn compile(&self, sess: &Session, schema: &Schema, p: &Pred, params: &[Value]) -> Result<CPred, String> {
        let off = schema.offsets[p.col]; let c = &schema.columns[p.col];
        let (nb, nm) = (p.col >> 3, 1u8 << (p.col & 7));
        let base = CPred { off, kind: 0, cmp: p.cmp, i: 0, f: 0.0, b: vec![], nb, nm, set: None, pair: None, corr_map: None, ckind: 0 };
        if let Expr::Scalar(sp) = &p.rhs { if let Some((_, oc)) = sp.corr {
            if !matches!(c.ty, ColType::Int | ColType::Float | ColType::Timestamp) { return Err(format!("column {}: correlated comparison needs a numeric column", c.name)); }
            let rows = self.run_select(sess, sp, params)?;
            let map = if schema.columns[oc].ty == ColType::Uuid {
                let mut m = crate::util::WideMap::default(); for r in rows { if let (Some(k), Ok(f)) = (uuid_key(&r[0]), r[1].as_f64()) { m.insert(k, f); } } CorrMap::W(m)
            } else { let mut m: FastMap<f64> = FastMap::default(); for r in rows { if let Value::Int(k) = r[0] { if let Ok(f) = r[1].as_f64() { m.insert(k, f); } } } CorrMap::N(m) };
            return Ok(CPred { kind: 20, pair: Some((schema.offsets[oc], oc >> 3, 1u8 << (oc & 7))), corr_map: Some(Arc::new(map)), ckind: if c.ty == ColType::Float { 1 } else { 0 }, ..base });
        } }
        if matches!(p.cmp, Cmp::IsNull | Cmp::IsNotNull) { return Ok(base); }
        if matches!(p.cmp, Cmp::In | Cmp::NotIn) {
            if c.ty == ColType::Uuid {
                let keys: Vec<Key> = match &p.rhs {
                    Expr::List(items) => { let mut ks = Vec::with_capacity(items.len()); for e in items { match self.resolve(sess, e, params)? { Value::Null => {}, v => ks.push(uuid_key(&v).ok_or_else(|| format!("column {}: not a UUID: {v:?}", c.name))?) } } ks }
                    Expr::Set(sp) => {
                        if sp.corr.is_some() && sp.proj.len() != 1 { return Err(format!("column {}: a correlated IN on a UUID column is not supported yet; use EXISTS or a JOIN", c.name)); }
                        self.run_select(sess, sp, params)?.iter().filter_map(|r| r.first().and_then(uuid_key)).collect()
                    }
                    _ => return Err("IN needs a list or a subquery".into()),
                };
                return Ok(CPred { kind: 4, set: Some(Arc::new(KeySet::build_wide(&keys))), ..base });
            }
            if !matches!(c.ty, ColType::Int | ColType::Timestamp) { return Err(format!("column {}: IN is supported on INT columns (for now)", c.name)); }
            let mut pair = None;
            let keys: Vec<i64> = match &p.rhs {
                Expr::List(items) => items.iter().map(|e| self.resolve(sess, e, params)).collect::<Result<Vec<_>, _>>()?.into_iter().filter_map(|v| if let Value::Int(i) = v { Some(i) } else { None }).collect(),
                Expr::Set(sp) => { let rows = self.run_select(sess, sp, params)?; match sp.corr {
                    None => rows.into_iter().filter_map(|r| if let Some(Value::Int(i)) = r.first() { Some(*i) } else { None }).collect(),
                    Some((_, oc)) => if sp.proj.len() == 1 { rows.into_iter().filter_map(|r| if let Some(Value::Int(i)) = r.first() { Some(*i) } else { None }).collect() }   // EXISTS: the set of correlated keys
                                     else { if schema.columns[oc].ty == ColType::Uuid { return Err(format!("column {}: a correlated IN keyed on a UUID column is not supported yet; use EXISTS or a JOIN", c.name)); }
                                            pair = Some((schema.offsets[oc], oc >> 3, 1u8 << (oc & 7))); rows.into_iter().filter_map(|r| match (&r[0], &r[1]) { (Value::Int(a), Value::Int(b)) => Some(mix_pair(*a, *b)), _ => None }).collect() } } }
                _ => return Err("IN needs a list or a subquery".into()),
            };
            return Ok(CPred { set: Some(Arc::new(KeySet::build(&keys))), pair, ..base });
        }
        let v = self.resolve(sess, &p.rhs, params)?;
        if v == Value::Null { return Ok(CPred { kind: 9, ..base }); } // comparison with NULL is never true
        Ok(match &c.ty {
            ColType::Int => CPred { kind: 0, i: v.as_i64().map_err(|e| format!("column {}: {e}", c.name))?, ..base },
            ColType::Timestamp => CPred { kind: 0, i: match &v { Value::Text(s) => crate::table::parse_timestamp(s).ok_or_else(|| format!("column {}: not a timestamp {s:?}", c.name))?, other => other.as_i64().map_err(|e| format!("column {}: {e}", c.name))? }, ..base },
            ColType::Float => CPred { kind: 1, f: v.as_f64().map_err(|e| format!("column {}: {e}", c.name))?, ..base },
            ColType::Bool => CPred { kind: 2, i: match v { Value::Bool(b) => b as i64, v => return Err(format!("column {}: expected BOOL, got {v:?}", c.name)) }, ..base },
            ColType::Text(_) => CPred { kind: 3, b: match v { Value::Text(s) => s.into_bytes(), v => return Err(format!("column {}: expected TEXT, got {v:?}", c.name)) }, ..base },
            ColType::Bytes(_) => CPred { kind: 3, b: match v { Value::Bytes(b) => b, v => return Err(format!("column {}: expected BYTES, got {v:?}", c.name)) }, ..base },
            ColType::Uuid => CPred { kind: 4, b: match &v { Value::Text(s) => crate::table::parse_uuid(s).ok_or_else(|| format!("column {}: not a UUID: {s:?}", c.name))?.to_vec(), Value::Bytes(b) if b.len() == 16 => b.clone(), v => return Err(format!("column {}: expected a UUID, got {v:?}", c.name)) }, ..base },
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
            Plan::Commit => { let t = sess.txn.take().ok_or("not in a transaction")?; sess.last_seq = self.db.commit(t).map_err(|a| a.to_string())?; Ok(vec![]) }
            Plan::Insert { table, rows, returning } => {
                let policy = self.guard(sess, table, P_INSERT)?;
                let schema = &table.schema; let mut encoded = Vec::with_capacity(rows.len()); let mut last_id = 0; let mut last_key = None;
                for exprs in rows {
                    let mut vals = Vec::with_capacity(exprs.len());
                    for e in exprs { vals.push(match e { Expr::AutoId => Value::Int(table.alloc_id()), Expr::Gen(g) => crate::table::gen_value(g), e => self.resolve(sess, e, params)? }); }
                    if let Value::Int(id) = vals[schema.pk] { if schema.auto { table.bump_id(id); } last_id = id; }
                    let row = encode_row(schema, &vals)?;
                    for p in &policy { if !p.eval(&row) { return Err(format!("permission denied: row violates a policy on {}", schema.name)); } }
                    last_key = Some(schema.key_of(&row));
                    encoded.push(row);
                }
                match &mut sess.txn {
                    Some(t) => { for row in &encoded { table.txn_insert(t, row).map_err(|a| a.to_string())?; } }
                    None if encoded.len() == 1 => { sess.last_seq = table.insert(&encoded[0]).map_err(|a| a.to_string())?; }
                    None => { let mut t = Txn::new(); for row in &encoded { table.txn_insert(&mut t, row).map_err(|a| a.to_string())?; } sess.last_seq = self.db.commit(t).map_err(|a| a.to_string())?; }   // several rows: all or none
                }
                sess.last_insert_id = last_id; if let Some(k) = last_key { sess.last_insert_key = Some(schema.key_value(k)); }
                Ok(match returning { None => vec![], Some(cols) => encoded.iter().map(|row| cols.iter().map(|&c| decode_col(schema, row, c)).collect()).collect() })
            }
            Plan::Update { table, sets, pk } => {
                let policy = self.guard(sess, table, P_UPDATE)?;
                let pk = table.schema.key_from_value(&self.resolve(sess, pk, params)?)?;
                let schema = &table.schema;
                let vals = sets.iter().map(|(c, e)| self.resolve(sess, e, params).map(|v| (*c, v))).collect::<Result<Vec<_>, _>>()?;
                let f = |row: &[u8]| -> Result<Vec<u8>, Abort> { for p in &policy { if !p.eval(row) { return Err(Abort::NotFound); } } let mut new = row.to_vec(); for (c, v) in &vals { encode_col(schema, &mut new, *c, v).map_err(Abort::Type)?; } for p in &policy { if !p.eval(&new) { return Err(Abort::Type("permission denied: new row violates a policy".into())); } } Ok(new) };
                match &mut sess.txn { Some(t) => { table.txn_update(t, pk, f).map_err(|a| a.to_string())?; } None => { sess.last_seq = table.update_by_pk(pk, f).map_err(|a| a.to_string())?; } }
                Ok(vec![])
            }
            Plan::Delete { table, access, preds } => {
                let mut policy = self.guard(sess, table, P_DELETE)?;
                let schema = &table.schema;
                let mut cp = preds.iter().map(|p| self.compile(sess, schema, p, params)).collect::<Result<Vec<_>, _>>()?; cp.append(&mut policy);
                let pk_off = schema.offsets[schema.pk];
                let mut pks: Vec<crate::table::Key> = vec![]; let _ = pk_off;
                let mut collect = |row: &[u8]| -> bool { for p in &cp { if !p.eval(row) { return true; } } pks.push(schema.key_of(row)); true };
                match access {
                    Access::Pk(e) => { let pk = schema.key_from_value(&self.resolve(sess, e, params)?)?; match &mut sess.txn { Some(t) => { if let Some(r) = table.txn_get(t, pk) { collect(&r); } } None => { table.with_pk(pk, |r| collect(r)); } } }
                    Access::Unique(e) => { let u = schema.unique.unwrap(); let ty = &schema.columns[u].ty; let v = self.resolve(sess, e, params)?; let mut key = vec![0u8; ty.width()]; encode_value(ty, &v, &mut key)?; table.with_unique(&key, |r| collect(r)); }
                    Access::Range { lo, lo_excl, hi, hi_excl } => { let mut a = self.resolve(sess, lo, params)?.as_i64()?; let mut b = self.resolve(sess, hi, params)?.as_i64()?; if *lo_excl { a += 1; } if *hi_excl { b -= 1; } table.range(a, b, &mut collect); }
                    Access::Index(ix, e) => { let k = index_key(schema, ix.col, &self.resolve(sess, e, params)?)?; table.index_lookup(ix, k, &mut collect); }
                    Access::Scan => table.scan(&mut collect),
                }
                let n = pks.len();
                match &mut sess.txn {
                    Some(t) => { for pk in pks { table.txn_delete(t, pk).map_err(|a| a.to_string())?; } }
                    None => if n == 1 { sess.last_seq = table.delete_by_pk(pks[0]).map_err(|a| a.to_string())?; } else if n > 1 { let mut t = Txn::new(); for pk in pks { table.txn_delete(&mut t, pk).map_err(|a| a.to_string())?; } sess.last_seq = self.db.commit(t).map_err(|a| a.to_string())?; }
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
            let wide = oc.is_some_and(|oc| schema.columns[oc].ty == ColType::Uuid);
            // selective outer side + an index on the inner correlation column: evaluate the inner per outer key through the index
            if let (Some(oc), Some((cv, _)), None, false) = (oc, sp.corr, &sp.join, wide) {
                if let (true, Some(ix), Some(Proj::Agg(agg))) = (outer_selective, sp.table.index_on(cv), sp.proj.get(1)) {
                    let pi = params.len();
                    let lazy = Arc::new(SelectPlan { table: sp.table.clone(), schema: sp.schema.clone(), access: Access::Index(ix, Expr::Param(pi)), preds: sp.preds.clone(), proj: vec![Proj::Agg(agg.clone())], names: vec![], group_by: None, order: None, limit: None, join: None, corr: None, subs: vec![] });
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
                Some(oc) if wide => { let mut map = crate::util::WideMap::default(); for r in rows { if let Some(k) = uuid_key(&r[0]) { map.insert(k, r[1].clone()); } } SubVal::WMap { map, off: schema.offsets[*oc], nb: oc >> 3, nm: 1u8 << (oc & 7), dflt } }
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
            Access::Pk(e) => { let pk = t.schema.key_from_value(&self.resolve(sess, e, params)?)?; match txn { Some(tx) => { if let Some(row) = t.txn_get(tx, pk) { visit(&row); } } None => { t.with_pk(pk, |r| visit(r)); } } }
            Access::Unique(e) => { let u = schema.unique.unwrap(); let ty = &schema.columns[u].ty; let v = self.resolve(sess, e, params)?; let mut key = vec![0u8; ty.width()]; encode_value(ty, &v, &mut key)?; t.with_unique(&key, |r| visit(r)); }
            Access::Range { lo, lo_excl, hi, hi_excl } => { let mut a = self.resolve(sess, lo, params)?.as_i64()?; let mut b = self.resolve(sess, hi, params)?.as_i64()?; if *lo_excl { a += 1; } if *hi_excl { b -= 1; } t.range(a, b, &mut visit); }
            Access::Index(ix, e) => { let k = index_key(&t.schema, ix.col, &self.resolve(sess, e, params)?)?; if covering && t.schema.columns[ix.col].ty != ColType::Uuid { let n = ix.count(crate::table::fold_key(k)) as u64; drop(visit); sink.push_count(&[], n); return Ok(sink.finish()); } t.index_lookup(ix, k, &mut visit); }
            Access::Scan => t.scan(&mut visit),
        }
        Ok(sink.finish())
    }

    /// Filter one join step's build table; returns (keys, compact rows [flags][cols], stride).
    fn collect_build(&self, sess: &Session, st: &JoinStep, params: &[Value], bp: &[CPred]) -> Result<(BuildKeys, Vec<u8>, usize), String> {
        let bs = &st.build.schema; let wide = bs.columns[st.build_key].ty == ColType::Uuid; let bkey_off = bs.offsets[st.build_key];
        let cols: Vec<(usize, usize, usize)> = st.build_cols.iter().map(|&c| (bs.offsets[c], bs.columns[c].ty.width(), c)).collect();
        let stride = cols.len() + cols.iter().map(|c| c.1).sum::<usize>();
        let (mut kn, mut kw): (Vec<i64>, Vec<crate::table::Key>) = (Vec::new(), Vec::new()); let mut arena: Vec<u8> = Vec::new();
        let mut collect = |row: &[u8]| -> bool { for p in bp { if !p.eval(row) { return true; } } if wide { kw.push(crate::table::key_from_uuid(&row[bkey_off..bkey_off + 16])); } else { kn.push(read_i64(row, bkey_off)); } for (_, _, c) in &cols { arena.push(is_null(row, *c) as u8); } for (off, w, _) in &cols { arena.extend_from_slice(&row[*off..*off + *w]); } true };
        match &st.build_access {
            Access::Pk(e) => { let pk = bs.key_from_value(&self.resolve(sess, e, params)?)?; st.build.with_pk(pk, |r| collect(r)); }
            Access::Range { lo, lo_excl, hi, hi_excl } => { let mut a = self.resolve(sess, lo, params)?.as_i64()?; let mut b = self.resolve(sess, hi, params)?.as_i64()?; if *lo_excl { a += 1; } if *hi_excl { b -= 1; } st.build.range(a, b, &mut collect); }
            Access::Index(ix, e) => { let k = index_key(bs, ix.col, &self.resolve(sess, e, params)?)?; st.build.index_lookup(ix, k, &mut collect); }
            _ => st.build.scan(&mut collect),
        }
        Ok((if wide { BuildKeys::W(kw) } else { BuildKeys::N(kn) }, arena, stride))
    }
    /// Key-addressable store over collected build rows: bitmap (no columns), dense array, or hash map.
    fn make_store(bk: &BuildKeys, arena: Vec<u8>, stride: usize) -> Store {
        if bk.len() == 0 { return Store::Empty; }
        let keys = match bk {
            BuildKeys::W(keys128) => {   // UUID keys: exact 128-bit lookups
                if stride == 0 { return Store::WideSet(keys128.iter().copied().collect()); }
                return Store::WideHash { map: keys128.iter().enumerate().map(|(i, &k)| (k, i as u32)).collect(), arena, stride };
            }
            BuildKeys::N(k) => &k[..],
        };
        let n = keys.len();
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
        let wide: Vec<bool> = j.steps.iter().map(|st| st.build.schema.columns[st.build_key].ty == ColType::Uuid).collect();
        let mut combined = vec![0u8; vschema.row_size];
        let fill_probe = |combined: &mut [u8], prow: &[u8]| { for b in &mut combined[..vn] { *b = 0; } combined[..ps.null_bytes].copy_from_slice(&prow[..ps.null_bytes]); combined[pbase..pbase + ps.row_size].copy_from_slice(prow); };
        let fill_build = |combined: &mut [u8], k: usize, brow: &[u8]| { let st = &j.steps[k]; let mut o = st.base; for (i, (off, w, c)) in layouts[k].iter().enumerate() { combined[o..o + w].copy_from_slice(&brow[*off..*off + *w]); if is_null(brow, *c) { let vi = st.vbase + i; combined[vi >> 3] |= 1 << (vi & 7); } o += w; } };
        let fill_entry = |combined: &mut [u8], k: usize, entry: &[u8]| { let st = &j.steps[k]; let nf = layouts[k].len(); combined[st.base..st.base + entry.len() - nf].copy_from_slice(&entry[nf..]); for i in 0..nf { if entry[i] != 0 { let vi = st.vbase + i; combined[vi >> 3] |= 1 << (vi & 7); } } };
        // Point mode: the probe side is selective, every step is a primary-key lookup.
        // Scan mode: filter every build side first; if one filtered side is small and the probe key is indexed, drive
        // the join through that index (expected probe rows = matched/total * probe rows); otherwise scan the probe.
        let point_mode = !matches!(j.probe_access, Access::Scan);
        let mut stores: Vec<Store> = vec![]; let mut driver: Option<(usize, BuildKeys, Vec<u8>, usize, Arc<SecIndex>)> = None;
        if !point_mode {
            let mut collected: Vec<(BuildKeys, Vec<u8>, usize)> = vec![];
            for (k, st) in j.steps.iter().enumerate() { collected.push(self.collect_build(sess, st, params, &bps[k])?); }
            let prow_n = j.probe.rows_allocated().max(1) as f64;
            let mut best = f64::MAX;
            for (k, st) in j.steps.iter().enumerate() {
                if st.key_vidx >= ps.columns.len() { continue; }
                if let Some(ix) = j.probe.index_on(st.key_vidx) {
                    let (m, b) = (collected[k].0.len() as f64, st.build.rows_allocated().max(1) as f64);
                    let expected = m / b * prow_n;                     // probe rows reached through the index
                    if expected * 40.0 < prow_n && expected < best { best = expected; driver = Some((k, BuildKeys::N(vec![]), vec![], 0, ix)); }   // an index-reached row costs ~40 scanned rows
                }
            }
            for (k, (keys, arena, stride)) in collected.into_iter().enumerate() {
                if let Some(d) = &mut driver { if d.0 == k { d.1 = keys; d.2 = arena; d.3 = stride; stores.push(Store::Empty); continue; } }
                stores.push(Self::make_store(&keys, arena, stride));
            }
        }
        if let Some((k, keys, arena, stride, ix)) = &driver {
            if pp.is_empty() && preds.is_empty() && j.steps.len() == 1 && !wide[*k] && sink.count_only() && sink.group_at_or_after(ps.columns.len()) {
                // covering: the index entry count per key IS the number of committed matching rows; no probe rows are read
                for i in 0..keys.len() { let n = ix.count(keys.get(i) as i64) as u64; if n > 0 { for b in &mut combined[..vn] { *b = 0; } fill_entry(&mut combined, *k, &arena[i * stride..(i + 1) * stride]); sink.push_count(&combined, n); } }
                return Ok(());
            }
        }
        let apply_step = |combined: &mut [u8], k: usize| -> bool {
            let st = &j.steps[k];
            if is_null(combined, st.key_vidx) { return false; }
            let key: crate::table::Key = if wide[k] { crate::table::key_from_uuid(&combined[key_offs[k]..key_offs[k] + 16]) } else { read_i64(combined, key_offs[k]) as i128 };
            if point_mode { return st.build.with_pk(key, |brow| { for p in &bps[k] { if !p.eval(brow) { return false; } } fill_build(combined, k, brow); true }).unwrap_or(false); }
            match &stores[k] {
                Store::Empty => false,
                Store::WideSet(set) => set.contains(&key),
                Store::WideHash { map, arena, stride } => match map.get(&key) { Some(&i) => { fill_entry(combined, k, &arena[i as usize * stride..(i as usize + 1) * stride]); true } None => false },
                Store::Set(set) => set.contains(key as i64),
                Store::Dense { min, max, es, data } => { let key = key as i64; if key < *min || key > *max { return false; } let o = (key - min) as usize * es; if data[o] == 0 { return false; } fill_entry(combined, k, &data[o + 1..o + es]); true }
                Store::Hash { map, arena, stride } => match map.get(&(key as i64)) { Some(&i) => { fill_entry(combined, k, &arena[i as usize * stride..(i as usize + 1) * stride]); true } None => false },
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
            for i in 0..keys.len() {
                let entry = &arena[i * stride..(i + 1) * stride];
                j.probe.index_lookup(&ix, keys.get(i), |prow| { for p in &pp { if !p.eval(prow) { return true; } } fill_probe(&mut combined, prow); fill_entry(&mut combined, k, entry); finish(&mut combined, k) });
                if stop.get() { break; }
            }
            return Ok(());
        }
        // reject on the raw probe row before copying anything: steps keyed by a probe column whose store is a plain key set
        let prechecks: Vec<(usize, usize, usize)> = j.steps.iter().enumerate().filter(|(k, st)| st.key_vidx < ps.columns.len() && !wide[*k] && matches!(stores.get(*k), Some(Store::Set(_)) | Some(Store::Empty))).map(|(k, st)| (k, st.key_vidx, ps.offsets[st.key_vidx])).collect();
        let mut per_probe = |prow: &[u8]| -> bool {
            for p in &pp { if !p.eval(prow) { return true; } }
            for &(k, col, off) in &prechecks { if is_null(prow, col) { return true; } match &stores[k] { Store::Set(set) => if !set.contains(read_i64(prow, off)) { return true; }, Store::Empty => return true, _ => {} } }
            fill_probe(&mut combined, prow); finish(&mut combined, usize::MAX)
        };
        match &j.probe_access {
            Access::Pk(e) => { let pk = ps.key_from_value(&self.resolve(sess, e, params)?)?; j.probe.with_pk(pk, |r| per_probe(r)); }
            Access::Unique(e) => { let u = ps.unique.unwrap(); let ty = &ps.columns[u].ty; let v = self.resolve(sess, e, params)?; let mut key = vec![0u8; ty.width()]; encode_value(ty, &v, &mut key)?; j.probe.with_unique(&key, |r| per_probe(r)); }
            Access::Range { lo, lo_excl, hi, hi_excl } => { let mut a = self.resolve(sess, lo, params)?.as_i64()?; let mut b = self.resolve(sess, hi, params)?.as_i64()?; if *lo_excl { a += 1; } if *hi_excl { b -= 1; } j.probe.range(a, b, &mut per_probe); }
            Access::Index(ix, e) => { let k = index_key(ps, ix.col, &self.resolve(sess, e, params)?)?; j.probe.index_lookup(ix, k, &mut per_probe); }
            Access::Scan => j.probe.scan(&mut per_probe),
        }
        Ok(())
    }
}
/// A join step's collected build keys, at their native width.
enum BuildKeys { N(Vec<i64>), W(Vec<crate::table::Key>) }
impl BuildKeys { fn len(&self) -> usize { match self { BuildKeys::N(v) => v.len(), BuildKeys::W(v) => v.len() } } #[inline] fn get(&self, i: usize) -> crate::table::Key { match self { BuildKeys::N(v) => v[i] as i128, BuildKeys::W(v) => v[i] } } }
enum Store { Empty, Set(KeySet), Dense { min: i64, max: i64, es: usize, data: Vec<u8> }, Hash { map: FastMap<u32>, arena: Vec<u8>, stride: usize }, WideSet(crate::util::WideSet), WideHash { map: crate::util::WideMap<u32>, arena: Vec<u8>, stride: usize } }


/// Restore the latest state of a bucket into an empty `dir` before the first open (`backup.bootstrap`). Downloads into
/// a scratch directory and moves the files over; a bucket with nothing in it leaves `dir` empty.
fn bootstrap_from_bucket(dir: &std::path::Path, b: &crate::config::Backup) -> Result<(), String> {
    let scratch = dir.join(".bootstrap");
    let (s0, chunks, bytes) = crate::cloud::download(&b.cloud(), &scratch)?;
    if s0 == 0 && chunks == 0 { let _ = std::fs::remove_dir_all(&scratch); return Ok(()); }
    for e in std::fs::read_dir(&scratch).map_err(|e| e.to_string())? { let e = e.map_err(|e| e.to_string())?; std::fs::rename(e.path(), dir.join(e.file_name())).map_err(|e| e.to_string())?; }
    let _ = std::fs::remove_dir_all(&scratch);
    eprintln!("sqweezdb: bootstrapped {} from the bucket ({chunks} log objects, {:.1} MB)", dir.display(), bytes as f64 / 1e6);
    Ok(())
}
/// The snapshot policy thread: a snapshot once the log written since the last one exceeds max(`log_bytes`, size of that
/// snapshot), or after `idle_after` without a write when something was written since the last one. Holds only a weak
/// handle, so it ends with the database.
fn spawn_snapshotter(db: &Arc<Database>, pol: crate::config::SnapshotPolicy, count: Arc<std::sync::atomic::AtomicU64>) -> (Arc<std::sync::atomic::AtomicBool>, std::thread::JoinHandle<()>) {
    // Holds the storage engine, not the database handle: the handle's close (and so its drop) stops and joins this
    // thread before the engine and the directory lock go, so dropping the last handle releases the directory on the
    // caller's thread. (A weak handle upgraded on this thread could make it the last owner, releasing the lock late.)
    use std::sync::atomic::Ordering;
    let db = db.clone(); let stop = Arc::new(std::sync::atomic::AtomicBool::new(false)); let stop2 = stop.clone();
    let mut snap_bytes = std::fs::metadata(db.dir.join("snap")).map(|m| m.len()).unwrap_or(0);
    let (mut base_bytes, mut snap_seq) = { let s = db.log.st.lock().unwrap(); (s.bytes, s.seq) };
    let h = std::thread::spawn(move || {
        let tick = std::time::Duration::from_millis(200);
        let (mut seen_seq, mut seen_at) = (snap_seq, std::time::Instant::now());
        loop {
            std::thread::sleep(tick);
            if stop2.load(Ordering::Relaxed) { return; }
            let (bytes, seq) = { let s = db.log.st.lock().unwrap(); (s.bytes, s.seq) };
            if seq != seen_seq { seen_seq = seq; seen_at = std::time::Instant::now(); }
            let by_size = pol.log_bytes.map(|n| bytes.saturating_sub(base_bytes) >= n.max(snap_bytes)).unwrap_or(false);
            let by_idle = pol.idle_after.map(|d| seq != snap_seq && seen_at.elapsed() >= d).unwrap_or(false);
            if by_size || by_idle { let st = db.snapshot(); snap_bytes = st.bytes; count.fetch_add(1, Ordering::Relaxed); let s = db.log.st.lock().unwrap(); base_bytes = s.bytes; snap_seq = s.seq; }
        }
    });
    (stop, h)
}

/// Hold `<dir>/.lock` with flock for as long as the database is open: a second owner, in this process or another,
/// is refused instead of corrupting the log.
fn lock_dir(dir: &std::path::Path) -> Result<std::fs::File, String> {
    std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    let f = std::fs::OpenOptions::new().read(true).write(true).create(true).open(dir.join(".lock")).map_err(|e| format!("{}: {e}", dir.join(".lock").display()))?;
    use std::os::unix::io::AsRawFd;
    // A previous owner that is closing (a process exiting, a handle whose last thread is finishing) gets 2 s.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        if std::time::Instant::now() >= deadline { return Err(format!("{} is already open (another process or handle holds {}/.lock)", dir.display(), dir.display())); }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    Ok(f)
}
#[cfg(feature = "ui")]
fn start_http(sql: &Arc<Sql>, addr: &str, token: &str) -> Result<crate::net::Listener, String> { crate::ui::start(sql.clone(), addr, token) }
#[cfg(not(feature = "ui"))]
fn start_http(_sql: &Arc<Sql>, _addr: &str, _token: &str) -> Result<crate::net::Listener, String> { Err("listen.http needs the `ui` feature (this library was built without it)".into()) }

#[cfg(test)]
mod dir_tests {
    use super::*;
    use crate::config::Config;
    use crate::table::{check_dir, read_meta, DirState, DIR_FORMAT, META_FILE};
    fn tmp(name: &str) -> std::path::PathBuf { let p = std::env::temp_dir().join(format!("sqweezdb-dirtest-{}-{name}", std::process::id())); let _ = std::fs::remove_dir_all(&p); let _ = std::fs::remove_file(&p); p }
    fn files(p: &std::path::Path) -> Vec<String> { let mut v: Vec<String> = std::fs::read_dir(p).map(|r| r.flatten().map(|e| e.file_name().to_string_lossy().into_owned()).collect()).unwrap_or_default(); v.sort(); v }
    #[test] fn new_directory_gets_a_marker_and_reopens() {
        let d = tmp("new");
        { let s = Sql::open_with(&d, Config::default()).unwrap(); s.exec_s(&mut Session::default(), "CREATE TABLE t(id INTEGER PRIMARY KEY)", &[]).unwrap(); }
        assert_eq!(read_meta(&d).unwrap(), Some(DIR_FORMAT)); assert_eq!(check_dir(&d).unwrap(), DirState::Database);
        assert!(std::fs::read_to_string(d.join(META_FILE)).unwrap().contains("created_by = \"sqweezdb "));
        let s = Sql::open_with(&d, Config::default()).unwrap(); assert!(s.db.table("t").is_some()); drop(s);
        std::fs::remove_dir_all(&d).unwrap();
    }
    #[test] fn a_file_is_refused() {
        let f = tmp("file"); std::fs::write(&f, b"hello").unwrap();
        let e = Sql::open_with(&f, Config::default()).err().unwrap(); assert!(e.contains("is a file"), "{e}"); assert_eq!(error_code(&e), "PATH");
        assert_eq!(std::fs::read(&f).unwrap(), b"hello"); std::fs::remove_file(&f).unwrap();
    }
    #[test] fn a_foreign_directory_is_refused_untouched() {
        let d = tmp("foreign"); std::fs::create_dir_all(d.join("src")).unwrap(); std::fs::write(d.join("package.json"), b"{}").unwrap();
        let before = files(&d);
        let e = Sql::open_with(&d, Config::default()).err().unwrap(); assert!(e.contains("is not a sqweezdb database and is not empty"), "{e}"); assert_eq!(error_code(&e), "PATH");
        assert_eq!(files(&d), before, "nothing may be written into a directory we refuse");
        std::fs::remove_dir_all(&d).unwrap();
    }
    #[test] fn hidden_and_toml_files_do_not_count() {
        let d = tmp("cfgonly"); std::fs::create_dir_all(&d).unwrap(); std::fs::write(d.join("orders.toml"), b"durable = true\n").unwrap(); std::fs::write(d.join(".gitkeep"), b"").unwrap();
        assert_eq!(check_dir(&d).unwrap(), DirState::Empty); drop(Sql::open_with(&d, Config::default()).unwrap());
        assert_eq!(read_meta(&d).unwrap(), Some(DIR_FORMAT)); std::fs::remove_dir_all(&d).unwrap();
    }
    #[test] fn an_unmarked_database_is_adopted() {
        let d = tmp("legacy");
        { let s = Sql::open_with(&d, Config::default()).unwrap(); s.exec_s(&mut Session::default(), "CREATE TABLE t(id INTEGER PRIMARY KEY)", &[]).unwrap(); s.exec_s(&mut Session::default(), "INSERT INTO t VALUES (7)", &[]).unwrap(); }
        std::fs::remove_file(d.join(META_FILE)).unwrap(); assert_eq!(check_dir(&d).unwrap(), DirState::Unmarked);
        let s = Sql::open_with(&d, Config::default()).unwrap(); assert_eq!(s.exec_s(&mut Session::default(), "SELECT count(*) FROM t", &[]).unwrap()[0][0], Value::Int(1)); drop(s);
        assert_eq!(read_meta(&d).unwrap(), Some(DIR_FORMAT), "the marker is written on adoption"); std::fs::remove_dir_all(&d).unwrap();
    }
    #[test] fn a_newer_format_and_a_broken_marker_are_refused() {
        let d = tmp("newer"); std::fs::create_dir_all(&d).unwrap(); std::fs::write(d.join(META_FILE), format!("format = {}\n", DIR_FORMAT + 1)).unwrap();
        let e = Sql::open_with(&d, Config::default()).err().unwrap(); assert!(e.contains("newer sqweezdb"), "{e}");
        std::fs::write(d.join(META_FILE), "format = \"one\"\n").unwrap(); let e = Sql::open_with(&d, Config::default()).err().unwrap(); assert!(e.contains("no format number"), "{e}");
        std::fs::write(d.join(META_FILE), "this is [not toml").unwrap(); let e = Sql::open_with(&d, Config::default()).err().unwrap(); assert!(e.contains("unreadable"), "{e}");
        std::fs::remove_dir_all(&d).unwrap();
    }
    #[test] fn create_off_refuses_missing_and_empty() {
        let d = tmp("nocreate"); let cfg = Config { create: false, ..Config::default() };
        assert!(Sql::open_with(&d, cfg.clone()).err().unwrap().contains("create is off")); assert!(!d.exists(), "nothing is created when create is off");
        std::fs::create_dir_all(&d).unwrap(); assert!(Sql::open_with(&d, cfg).err().unwrap().contains("create is off")); std::fs::remove_dir_all(&d).unwrap();
    }
    #[test] fn a_failing_alter_is_an_error_and_changes_nothing() {
        let d = tmp("alter"); let s = Sql::open_with(&d, Config::default()).unwrap(); let mut sess = Session::default();
        s.exec_s(&mut sess, "CREATE TABLE t(id INTEGER PRIMARY KEY, score DOUBLE, name VARCHAR(8))", &[]).unwrap();
        s.exec_s(&mut sess, "INSERT INTO t VALUES (1, 1.5, 'a')", &[]).unwrap();
        let e = s.exec_s(&mut sess, "ALTER TABLE t ALTER COLUMN score SET DATA TYPE INT", &[]).err().expect("1.5 cannot become an INT");
        assert!(e.contains("was not applied"), "{e}");
        assert_eq!(s.db.table("t").unwrap().schema.columns[1].ty, crate::table::ColType::Float, "the type is unchanged");
        // several operations in one statement: all or nothing
        let e = s.exec_s(&mut sess, "ALTER TABLE t ADD COLUMN extra INT, ALTER COLUMN score SET DATA TYPE INT", &[]).err().expect("must fail");
        assert!(e.contains("was not applied"), "{e}"); assert!(s.db.table("t").unwrap().schema.col("extra").is_none(), "nothing applied");
        // and a valid change is visible as soon as the statement returns
        s.exec_s(&mut sess, "ALTER TABLE t RENAME COLUMN name TO label, ADD COLUMN extra INT", &[]).unwrap();
        let t = s.db.table("t").unwrap(); assert!(t.schema.col("label").is_some() && t.schema.col("extra").is_some());
        assert_eq!(s.exec_s(&mut sess, "SELECT label FROM t WHERE id = 1", &[]).unwrap()[0][0], Value::Text("a".into()));
        drop(s); std::fs::remove_dir_all(&d).unwrap();
    }
    #[test] fn follow_dir_must_be_a_database() {
        let src = tmp("follow-src"); std::fs::create_dir_all(&src).unwrap(); let dst = tmp("follow-dst");
        let cfg = Config { follow: Some(crate::config::Follow::Dir { dir: src.clone(), poll: std::time::Duration::from_millis(20) }), ..Config::default() };
        assert!(Sql::open_with(&dst, cfg).err().unwrap().contains("holds no sqweezdb database")); std::fs::remove_dir_all(&src).unwrap();
    }
}

#[cfg(test)]
mod gen_tests {
    use super::*;
    use crate::config::Config;
    fn tmp(name: &str) -> std::path::PathBuf { let p = std::env::temp_dir().join(format!("sqweezdb-gentest-{}-{name}", std::process::id())); let _ = std::fs::remove_dir_all(&p); p }
    fn q(s: &Sql, sql: &str) -> Vec<Vec<Value>> { s.exec_s(&mut Session::default(), sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}")) }
    fn int(v: &Value) -> i64 { match v { Value::Int(i) => *i, v => panic!("not an int: {v:?}") } }
    #[test] fn auto_ids_fill_in_and_respect_explicit_keys() {
        let d = tmp("auto"); let s = Sql::open_with(&d, Config::default()).unwrap(); let mut sess = Session::default();
        s.exec_s(&mut sess, "CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR(16))", &[]).unwrap();
        s.exec_s(&mut sess, "INSERT INTO t (name) VALUES ('a')", &[]).unwrap(); assert_eq!(sess.last_insert_id, 1);
        s.exec_s(&mut sess, "INSERT INTO t VALUES (10, 'explicit')", &[]).unwrap();
        s.exec_s(&mut sess, "INSERT INTO t (name) VALUES ('b')", &[]).unwrap(); assert_eq!(sess.last_insert_id, 11, "continues above an explicit key");
        s.exec_s(&mut sess, "INSERT INTO t VALUES (NULL, 'null id')", &[]).unwrap(); assert_eq!(sess.last_insert_id, 12, "NULL asks for an automatic key");
        s.exec_s(&mut sess, "INSERT INTO t VALUES (DEFAULT, 'default id')", &[]).unwrap(); assert_eq!(sess.last_insert_id, 13, "DEFAULT asks for an automatic key");
        for (ddl, name) in [("CREATE TABLE s1(id SERIAL PRIMARY KEY, v INT)", "s1"), ("CREATE TABLE s2(id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, v INT)", "s2"), ("CREATE TABLE s3(id INT AUTO_INCREMENT PRIMARY KEY, v INT)", "s3")] {
            q(&s, ddl); let r = q(&s, &format!("INSERT INTO {name} (v) VALUES (7), (8) RETURNING id")); assert_eq!(r, vec![vec![Value::Int(1)], vec![Value::Int(2)]], "{ddl}");
        }
        assert!(s.exec_s(&mut sess, "CREATE TABLE bad(id INT PRIMARY KEY, n INT AUTOINCREMENT)", &[]).unwrap_err().contains("not the primary key"));
        assert!(s.exec_s(&mut sess, "INSERT INTO t (name) VALUES (NULL)", &[]).is_ok(), "a nullable column may be NULL");
        drop(s); std::fs::remove_dir_all(&d).unwrap();
    }
    #[test] fn deleted_ids_are_not_reused_after_restart() {
        let d = tmp("reuse");
        { let s = Sql::open_with(&d, Config::default()).unwrap(); q(&s, "CREATE TABLE t(id SERIAL PRIMARY KEY, v INT)"); q(&s, "INSERT INTO t (v) VALUES (1), (2), (3)"); q(&s, "DELETE FROM t WHERE id = 3"); }
        { let s = Sql::open_with(&d, Config::default()).unwrap(); let r = q(&s, "INSERT INTO t (v) VALUES (4) RETURNING id"); assert_eq!(int(&r[0][0]), 4, "log replay remembers the deleted 3");
          q(&s, "DELETE FROM t WHERE id = 4"); s.snapshot(); }
        { let s = Sql::open_with(&d, Config::default()).unwrap(); let r = q(&s, "INSERT INTO t (v) VALUES (5) RETURNING id"); assert_eq!(int(&r[0][0]), 5, "the snapshot remembers the deleted 4");
          q(&s, "ALTER TABLE t ADD COLUMN note VARCHAR(8)"); let r = q(&s, "INSERT INTO t (v) VALUES (6) RETURNING id"); assert_eq!(int(&r[0][0]), 6, "an online ALTER keeps the sequence"); }
        std::fs::remove_dir_all(&d).unwrap();
    }
    #[test] fn a_wrong_number_of_values_names_the_columns_and_the_fix() {
        let d = std::env::temp_dir().join(format!("sqweezdb-valcount-{}", std::process::id())); let _ = std::fs::remove_dir_all(&d);
        let s = Sql::open_with(&d, crate::config::Config::default()).unwrap();
        let err = |sql: &str| s.exec_s(&mut Session::default(), sql, &[]).unwrap_err();
        s.exec_s(&mut Session::default(), "CREATE TABLE people (id UUID PRIMARY KEY DEFAULT uuid(), name VARCHAR(32), at TIMESTAMP DEFAULT now())", &[]).unwrap();
        s.exec_s(&mut Session::default(), "CREATE TABLE notes (id SERIAL PRIMARY KEY, body VARCHAR(32), tag VARCHAR(8))", &[]).unwrap();
        let e = err("INSERT INTO people VALUES ('ilya')");
        assert_eq!(e, "table people has 3 columns (id, name, at) but VALUES gives 1 value; name the columns you fill, as in INSERT INTO people (name) VALUES (...), or write DEFAULT for id, at");
        assert_eq!(error_code(&e), "SYNTAX");
        let e = err("INSERT INTO notes VALUES ('x')");
        assert_eq!(e, "table notes has 3 columns (id, body, tag) but VALUES gives 1 value; name the columns you fill, as in INSERT INTO notes (column, ...) VALUES (...), and leave out the ones with a default");
        assert_eq!(err("INSERT INTO notes VALUES (1, 'a', 'b', 'c')"), "table notes has 3 columns (id, body, tag) but VALUES gives 4 values");
        assert_eq!(err("INSERT INTO notes (body, tag) VALUES ('a', 'b'), ('c')"), "INSERT names 2 columns (body, tag) but row 2 of VALUES gives 1 value");
        assert_eq!(error_code(&err("INSERT INTO notes (body) VALUES ('a', 'b')")), "SYNTAX");
        drop(s); std::fs::remove_dir_all(&d).unwrap();
    }
    #[test] fn multi_row_insert_is_all_or_nothing_and_returns_every_row() {
        let d = tmp("multi"); let s = Sql::open_with(&d, Config::default()).unwrap();
        q(&s, "CREATE TABLE t(id INT PRIMARY KEY, v VARCHAR(8))");
        let r = q(&s, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c') RETURNING *"); assert_eq!(r.len(), 3); assert_eq!(r[2], vec![Value::Int(3), Value::Text("c".into())]);
        assert_eq!(int(&q(&s, "SELECT count(*) FROM t")[0][0]), 3, "every row of VALUES is stored");
        assert!(s.exec_s(&mut Session::default(), "INSERT INTO t VALUES (4, 'd'), (1, 'dup')", &[]).is_err());
        assert_eq!(int(&q(&s, "SELECT count(*) FROM t")[0][0]), 3, "a failing row stores none of them");
        let p = s.prepare("INSERT INTO t VALUES (?, ?) RETURNING id, v").unwrap(); assert_eq!(s.columns(&p).iter().map(|c| c.0.as_str()).collect::<Vec<_>>(), ["id", "v"]);
        drop(s); std::fs::remove_dir_all(&d).unwrap();
    }
    #[test] fn uuids_generate_store_and_match() {
        let d = tmp("uuid"); let s = Sql::open_with(&d, Config::default()).unwrap();
        q(&s, "CREATE TABLE u(id SERIAL PRIMARY KEY, uid UUID UNIQUE NOT NULL DEFAULT uuid(), r UUID DEFAULT gen_random_uuid(), at TIMESTAMP DEFAULT now(), name VARCHAR(8))");
        let before = crate::table::now_micros();
        for i in 0..2000 { q(&s, &format!("INSERT INTO u (name) VALUES ('n{i}')")); }
        let rows = q(&s, "SELECT id, uid, r, at FROM u");
        let mut uids: Vec<(i64, String)> = rows.iter().map(|r| (int(&r[0]), match &r[1] { Value::Text(t) => t.clone(), v => panic!("{v:?}") })).collect(); uids.sort();
        let set: std::collections::HashSet<&String> = uids.iter().map(|(_, u)| u).collect(); assert_eq!(set.len(), 2000, "all distinct");
        for (_, u) in &uids { assert_eq!(u.len(), 36); assert_eq!(&u[14..15], "7", "version 7: {u}"); assert!(matches!(&u[19..20], "8" | "9" | "a" | "b"), "RFC variant: {u}"); }
        assert!(uids.windows(2).all(|w| w[0].1 < w[1].1), "v7 UUIDs sort in insertion order");
        for r in &rows { let Value::Text(t) = &r[2] else { panic!() }; assert_eq!(&t[14..15], "4", "gen_random_uuid() is version 4"); let at = int(&r[3]); assert!(at >= before && at <= crate::table::now_micros(), "now() is the insert time"); }
        // look up by the unique index and by a scan, in any accepted spelling
        let (id, u) = uids[777].clone();
        let mut sess = Session::default();
        assert_eq!(int(&s.exec_s(&mut sess, "SELECT id FROM u WHERE uid = ?", &[Value::Text(u.to_uppercase())]).unwrap()[0][0]), id);
        assert_eq!(int(&s.exec_s(&mut sess, "SELECT id FROM u WHERE uid = ?", &[Value::Text(u.replace('-', ""))]).unwrap()[0][0]), id);
        let r777 = match &rows.iter().find(|r| int(&r[0]) == id).unwrap()[2] { Value::Text(t) => t.clone(), _ => panic!() };
        assert_eq!(int(&s.exec_s(&mut sess, "SELECT id FROM u WHERE r = ?", &[Value::Text(r777)]).unwrap()[0][0]), id, "scan comparison");
        // explicit values, bad values, wrong defaults
        q(&s, "INSERT INTO u (uid, name) VALUES ('{0190F3A2-0000-7000-8000-00000000ABCD}', 'given')");
        assert_eq!(q(&s, "SELECT uid FROM u WHERE name = 'given'")[0][0], Value::Text("0190f3a2-0000-7000-8000-00000000abcd".into()));
        let e = s.exec_s(&mut sess, "INSERT INTO u (uid, name) VALUES ('not-a-uuid', 'x')", &[]).unwrap_err(); assert!(e.contains("not a UUID"), "{e}"); assert_eq!(error_code(&e), "TYPE");
        assert!(s.exec_s(&mut sess, "INSERT INTO u (uid, name) VALUES (?, 'dup')", &[Value::Text(u.clone())]).unwrap_err().contains("Duplicate"), "UNIQUE holds for UUIDs");
        assert!(s.exec_s(&mut sess, "CREATE TABLE w(id INT PRIMARY KEY, n INT DEFAULT uuid())", &[]).unwrap_err().contains("needs a UUID column"));
        assert!(s.exec_s(&mut sess, "CREATE TABLE w(id INT PRIMARY KEY, n UUID DEFAULT 'zzz')", &[]).unwrap_err().contains("not a UUID"), "a constant default is checked up front");
        // ALTER: a new UUID column gets a distinct value in every existing row
        q(&s, "ALTER TABLE u ADD COLUMN public UUID NOT NULL DEFAULT uuid()");
        let p: std::collections::HashSet<String> = q(&s, "SELECT public FROM u").into_iter().map(|r| format!("{:?}", r[0])).collect(); assert_eq!(p.len(), 2001);
        drop(s);
        let s = Sql::open_with(&d, Config::default()).unwrap(); assert_eq!(int(&s.exec_s(&mut sess, "SELECT id FROM u WHERE uid = ?", &[Value::Text(u)]).unwrap()[0][0]), id, "after a restart");
        drop(s); std::fs::remove_dir_all(&d).unwrap();
    }
}

#[cfg(test)]
mod uuid_key_tests {
    use super::*;
    use crate::config::Config;
    fn tmp(name: &str) -> std::path::PathBuf { let p = std::env::temp_dir().join(format!("sqweezdb-uuidkey-{}-{name}", std::process::id())); let _ = std::fs::remove_dir_all(&p); p }
    fn q(s: &Sql, sql: &str) -> Vec<Vec<Value>> { s.exec_s(&mut Session::default(), sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}")) }
    fn qp(s: &Sql, sql: &str, p: &[Value]) -> Result<Vec<Vec<Value>>, String> { s.exec_s(&mut Session::default(), sql, p) }
    fn text(v: &Value) -> String { match v { Value::Text(t) => t.clone(), v => panic!("not text: {v:?}") } }
    fn int(v: &Value) -> i64 { match v { Value::Int(i) => *i, v => panic!("not an int: {v:?}") } }
    #[test] fn a_feed_reads_past_an_alter_copy() {
        let d = tmp("feedalter"); let s = Sql::open_with(&d, Config::default()).unwrap();
        q(&s, "CREATE TABLE t(id INT PRIMARY KEY, v INT)");
        for i in 0..200 { q(&s, &format!("INSERT INTO t VALUES ({}, {})", i * 1000 + 1, i)); }
        let ins = s.prepare("INSERT INTO t VALUES (?, 1)").unwrap(); let mut sess = Session::default();
        for i in 0..300_000i64 { if i % 1000 == 0 { s.exec_s(&mut sess, "BEGIN", &[]).unwrap(); } s.execute_s(&mut sess, &ins, &[Value::Int(1_000_000 + i)]).unwrap(); if i % 1000 == 999 { s.exec_s(&mut sess, "COMMIT", &[]).unwrap(); } }
        let _keep = s.subscribe(0);   // keeps the in-memory ring on
        let from = s.current_seq();
        q(&s, "ALTER TABLE t ADD COLUMN flag BOOL NOT NULL DEFAULT FALSE");   // ~300k copy records the feed must skip
        q(&s, "INSERT INTO t (id, v) VALUES (77000001, 1)");
        let mut f = s.subscribe(from); let mut got = vec![]; loop { let ch = f.poll(); if ch.is_empty() { break; } got.extend(ch); }
        assert_eq!(got.iter().map(|c| c.pk.clone()).collect::<Vec<_>>(), vec![Value::Int(77000001)], "the one real change, found past the copy");
        drop(f); drop(s); std::fs::remove_dir_all(&d).unwrap();
    }
    #[test] fn uuid_indexes_survive_a_restart() {
        let d = tmp("idx");
        let (a, b) = ("0190f3a2-0000-7000-8000-00000000000a".to_string(), "0190f3a2-0000-7000-8000-00000000000b".to_string());
        { let s = Sql::open_with(&d, Config::default()).unwrap();
          q(&s, "CREATE TABLE t(id INT PRIMARY KEY, owner UUID)");
          for i in 0..5000 { qp(&s, "INSERT INTO t VALUES (?, ?)", &[Value::Int(i), if i % 10 == 0 { Value::Null } else { Value::Text(if i % 2 == 0 { a.clone() } else { b.clone() }) }]).unwrap(); }
          q(&s, "CREATE INDEX t_owner ON t(owner)");   // filled from existing rows
          assert_eq!(int(&qp(&s, "SELECT count(*) FROM t WHERE owner = ?", &[Value::Text(a.clone())]).unwrap()[0][0]), 2000);
          s.snapshot(); }
        let s = Sql::open_with(&d, Config::default()).unwrap();   // rebuilt from the mapped snapshot
        let t = std::time::Instant::now();
        assert_eq!(int(&qp(&s, "SELECT count(*) FROM t WHERE owner = ?", &[Value::Text(a)]).unwrap()[0][0]), 2000);
        assert_eq!(int(&qp(&s, "SELECT count(*) FROM t WHERE owner = ?", &[Value::Text(b)]).unwrap()[0][0]), 2500);
        assert!(t.elapsed() < std::time::Duration::from_millis(100));
        drop(s); std::fs::remove_dir_all(&d).unwrap();
    }
    #[test] fn uuid_primary_keys_work_end_to_end() {
        let d = tmp("e2e");
        let (some_user, users_n, orders_n);
        {
            let s = Sql::open_with(&d, Config::default()).unwrap();
            q(&s, "CREATE TABLE users(id UUID PRIMARY KEY DEFAULT uuid(), name VARCHAR(16))");
            q(&s, "CREATE TABLE orders(id UUID PRIMARY KEY DEFAULT gen_random_uuid(), user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE, amount DOUBLE)");
            q(&s, "CREATE TABLE notes(id INT PRIMARY KEY, user_id UUID REFERENCES users(id), body VARCHAR(16))");   // RESTRICT
            // keys: generated, NULL, explicit; RETURNING and the session's last key
            let mut sess = Session::default();
            let r = s.exec_s(&mut sess, "INSERT INTO users (name) VALUES ('ann') RETURNING id", &[]).unwrap(); let ann = text(&r[0][0]);
            assert_eq!(sess.last_insert_key, Some(Value::Text(ann.clone()))); assert_eq!(&ann[14..15], "7");
            let bo = text(&q(&s, "INSERT INTO users VALUES (NULL, 'bo') RETURNING id")[0][0]); assert_ne!(ann, bo);
            q(&s, "INSERT INTO users VALUES ('0190F3A2-0000-7000-8000-0000000000AA', 'cy')");
            let cy = "0190f3a2-0000-7000-8000-0000000000aa".to_string();
            assert!(qp(&s, "INSERT INTO users VALUES (?, 'dup')", &[Value::Text(cy.replace('-', ""))]).unwrap_err().contains("Duplicate"), "the key is unique in any spelling");
            // bulk: enough rows for many pages and key shards
            for i in 0..20_000 { q(&s, &format!("INSERT INTO users (name) VALUES ('u{i}')")); }
            for i in 0..20_000 { let uid = if i % 3 == 0 { &ann } else if i % 3 == 1 { &bo } else { &cy }; qp(&s, "INSERT INTO orders (user_id, amount) VALUES (?, ?)", &[Value::Text(uid.clone()), Value::Float(i as f64)]).unwrap(); }
            // lookups, update, delete by key
            assert_eq!(text(&qp(&s, "SELECT name FROM users WHERE id = ?", &[Value::Text(ann.to_uppercase())]).unwrap()[0][0]), "ann");
            qp(&s, "UPDATE users SET name = 'ann2' WHERE id = ?", &[Value::Text(ann.clone())]).unwrap();
            assert_eq!(text(&qp(&s, "SELECT name FROM users WHERE id = ?", &[Value::Text(ann.clone())]).unwrap()[0][0]), "ann2");
            // foreign keys: the parent must exist; restrict blocks; the error names the UUID
            let e = qp(&s, "INSERT INTO orders (user_id, amount) VALUES ('0190f3a2-0000-7000-8000-00000000dead', 1)", &[]).unwrap_err(); assert!(e.contains("0190f3a2-0000-7000-8000-00000000dead"), "{e}");
            assert!(s.exec_s(&mut sess, "CREATE TABLE bad(id INT PRIMARY KEY, u INT REFERENCES users(id))", &[]).unwrap_err().contains("must be UUID"));
            assert!(s.exec_s(&mut sess, "CREATE TABLE bad2(id UUID PRIMARY KEY AUTOINCREMENT)", &[]).unwrap_err().contains("for a UUID key use DEFAULT uuid()"));
            qp(&s, "INSERT INTO notes VALUES (1, ?, 'hi')", &[Value::Text(cy.clone())]).unwrap();
            assert!(qp(&s, "DELETE FROM users WHERE id = ?", &[Value::Text(cy.clone())]).unwrap_err().contains("blocked"), "RESTRICT");
            // joins: one row by key, and all of them
            let oid = text(&q(&s, "SELECT id FROM orders WHERE amount = 5")[0][0]);
            let r = qp(&s, "SELECT o.amount, u.name FROM orders o JOIN users u ON o.user_id = u.id WHERE o.id = ?", &[Value::Text(oid)]).unwrap(); assert_eq!(r, vec![vec![Value::Float(5.0), Value::Text("cy".into())]]);
            assert_eq!(int(&q(&s, "SELECT count(*) FROM orders o JOIN users u ON o.user_id = u.id")[0][0]), 20_000);
            assert_eq!(int(&q(&s, "SELECT count(*) FROM orders o JOIN users u ON o.user_id = u.id WHERE u.name = 'bo'")[0][0]), 6_667);
            // a range on a UUID key is a filter; v7 keys sort by creation time
            assert_eq!(int(&qp(&s, "SELECT count(*) FROM users WHERE id > ?", &[Value::Text(bo.clone())]).unwrap()[0][0]), 20_000, "the 20k later users sort after bo");
            // secondary index on a UUID column, and the change feed carries UUID keys
            q(&s, "CREATE TABLE tags(id INT PRIMARY KEY, owner UUID, t VARCHAR(8))"); q(&s, "CREATE INDEX tags_owner ON tags(owner)");
            for i in 0..100 { qp(&s, "INSERT INTO tags VALUES (?, ?, 'x')", &[Value::Int(i), Value::Text(if i % 2 == 0 { ann.clone() } else { bo.clone() })]).unwrap(); }
            assert_eq!(int(&qp(&s, "SELECT count(*) FROM tags WHERE owner = ?", &[Value::Text(ann.clone())]).unwrap()[0][0]), 50);
            let s = Arc::new(s); let sub = s.reactor().subscribe(&Session::default(), "SELECT * FROM users WHERE name = 'live'", &[]).unwrap();
            let lid = text(&q(&s, "INSERT INTO users (name) VALUES ('live') RETURNING id")[0][0]);
            match sub.recv_timeout(std::time::Duration::from_secs(2)) { Some(crate::live::Event::Change(c)) => assert_eq!(c.pk, Value::Text(lid.clone())), other => panic!("no event: {:?}", other.is_some()) }
            // cascade
            let before = int(&q(&s, "SELECT count(*) FROM orders")[0][0]);
            qp(&s, "DELETE FROM users WHERE id = ?", &[Value::Text(bo.clone())]).unwrap();
            assert_eq!(int(&q(&s, "SELECT count(*) FROM orders")[0][0]), before - 6_667, "orders of bo cascaded");
            some_user = ann; users_n = int(&q(&s, "SELECT count(*) FROM users")[0][0]); orders_n = int(&q(&s, "SELECT count(*) FROM orders")[0][0]);
            drop(sub); s.close();
        }
        // restart from the log, then snapshot, then restart from the mapped snapshot
        for round in ["log", "snapshot"] {
            let s = Sql::open_with(&d, Config::default()).unwrap();
            assert_eq!(int(&q(&s, "SELECT count(*) FROM users")[0][0]), users_n, "{round}"); assert_eq!(int(&q(&s, "SELECT count(*) FROM orders")[0][0]), orders_n, "{round}");
            assert_eq!(text(&qp(&s, "SELECT name FROM users WHERE id = ?", &[Value::Text(some_user.clone())]).unwrap()[0][0]), "ann2", "{round}");
            let all: Vec<String> = q(&s, "SELECT id FROM users").into_iter().map(|r| text(&r[0])).collect();
            for k in all.iter().step_by(997) { assert_eq!(qp(&s, "SELECT id FROM users WHERE id = ?", &[Value::Text(k.clone())]).unwrap().len(), 1, "{round}: every key is found ({k})"); }
            assert_eq!(int(&q(&s, "SELECT count(*) FROM orders o JOIN users u ON o.user_id = u.id")[0][0]), orders_n, "{round}");
            if round == "log" { s.snapshot(); }
            drop(s);
        }
        std::fs::remove_dir_all(&d).unwrap();
    }
    /// Query shapes over UUID columns that used to read the first 8 bytes as an integer (wrong groups, no sort,
    /// one count for every parent), and the same shapes over TEXT/BOOL columns.
    #[test] fn uuid_columns_group_sort_and_correlate() {
        let d = tmp("shapes"); let s = Sql::open_with(&d, Config::default()).unwrap();
        let (u1, u2, u3) = ("00000000-0000-4000-8000-000000000001", "00000000-0000-4000-8000-000000000002", "00000000-0000-4000-8000-000000000003");
        q(&s, "CREATE TABLE a (id UUID PRIMARY KEY DEFAULT uuid(), name VARCHAR(16), ok BOOL, lim INT)");
        q(&s, "CREATE TABLE o (id UUID PRIMARY KEY DEFAULT uuid(), aid UUID REFERENCES a(id), amt INT)");
        q(&s, &format!("INSERT INTO a VALUES ('{u3}', 'carol', true, 0), ('{u1}', 'alice', false, 10), ('{u2}', 'bob', true, 10)"));
        q(&s, &format!("INSERT INTO o (aid, amt) VALUES ('{u1}', 5), ('{u1}', 7), ('{u2}', 1), (NULL, 4)"));
        let rows = |sql: &str| q(&s, sql).iter().map(|r| r.iter().map(|v| format!("{v:?}")).collect::<Vec<_>>().join(",")).collect::<Vec<_>>();
        let t = |x: &str| format!("Text({x:?})");
        // GROUP BY a UUID column: one group per id, NULL first, typed values; ORDER BY DESC reorders the groups
        assert_eq!(rows("SELECT aid, count(*) FROM o GROUP BY aid"), vec!["Null,Int(1)".to_string(), format!("{},Int(2)", t(u1)), format!("{},Int(1)", t(u2))]);
        assert_eq!(rows("SELECT aid, sum(amt) FROM o GROUP BY aid ORDER BY aid DESC"), vec![format!("{},Int(1)", t(u2)), format!("{},Int(12)", t(u1)), "Null,Int(4)".to_string()]);
        // GROUP BY TEXT (through a join) and BOOL
        assert_eq!(rows("SELECT a.name, count(*) FROM o JOIN a ON a.id = o.aid GROUP BY a.name"), vec![format!("{},Int(2)", t("alice")), format!("{},Int(1)", t("bob"))]);
        assert_eq!(rows("SELECT ok, count(*) FROM a GROUP BY ok"), vec!["Bool(false),Int(1)", "Bool(true),Int(2)"]);
        // ORDER BY a UUID or TEXT column sorts
        assert_eq!(rows("SELECT name, id FROM a ORDER BY id").iter().map(|r| r.split(',').next().unwrap().to_string()).collect::<Vec<_>>(), vec![t("alice"), t("bob"), t("carol")]);
        assert_eq!(rows("SELECT name, lim FROM a ORDER BY name DESC LIMIT 2"), vec![format!("{},Int(0)", t("carol")), format!("{},Int(10)", t("bob"))]);
        // correlated subqueries keyed on UUIDs: per-parent count and sum, EXISTS / NOT EXISTS, a comparison
        assert_eq!(rows("SELECT name, (SELECT count(*) FROM o WHERE o.aid = a.id) FROM a ORDER BY name"), vec![format!("{},Int(2)", t("alice")), format!("{},Int(1)", t("bob")), format!("{},Int(0)", t("carol"))]);
        assert_eq!(rows("SELECT name, (SELECT sum(amt) FROM o WHERE o.aid = a.id) FROM a ORDER BY name"), vec![format!("{},Int(12)", t("alice")), format!("{},Int(1)", t("bob")), format!("{},Null", t("carol"))]);
        assert_eq!(rows("SELECT name FROM a WHERE EXISTS (SELECT 1 FROM o WHERE o.aid = a.id) ORDER BY name"), vec![t("alice"), t("bob")]);
        assert_eq!(rows("SELECT name FROM a WHERE NOT EXISTS (SELECT 1 FROM o WHERE o.aid = a.id)"), vec![t("carol")]);
        assert_eq!(rows("SELECT name FROM a WHERE lim < (SELECT sum(amt) FROM o WHERE o.aid = a.id)"), vec![t("alice")]);
        // IN / NOT IN over UUIDs: a list, parameters, a subquery
        assert_eq!(rows(&format!("SELECT name FROM a WHERE id NOT IN ('{u1}', '{u3}')")), vec![t("bob")]);
        assert_eq!(qp(&s, "SELECT count(*) FROM a WHERE id IN (?, ?, NULL)", &[Value::Text(u1.into()), Value::Text(u3.into())]).unwrap(), vec![vec![Value::Int(2)]]);
        assert_eq!(rows("SELECT name FROM a WHERE id IN (SELECT aid FROM o) ORDER BY name"), vec![t("alice"), t("bob")]);
        assert!(qp(&s, "SELECT name FROM a WHERE id IN ('nope')", &[]).unwrap_err().contains("not a UUID"));
        // shapes that cannot be answered exactly are refused, not answered wrong
        assert!(qp(&s, "SELECT amt FROM o WHERE amt IN (SELECT lim FROM a WHERE a.id = o.aid)", &[]).unwrap_err().contains("not supported yet"));
        assert!(qp(&s, "SELECT name FROM a WHERE EXISTS (SELECT 1 FROM o WHERE o.amt = a.name)", &[]).unwrap_err().contains("unsupported correlation"));
        assert!(qp(&s, "SELECT name FROM a WHERE EXISTS (SELECT 1 FROM o WHERE o.amt = a.id)", &[]).unwrap_err().contains("unsupported correlation"));
        drop(s); std::fs::remove_dir_all(&d).unwrap();
    }
}

#[cfg(test)]
mod identity_tests {
    use super::*;
    use crate::config::Config;
    fn rows(s: &Sql, sess: &mut Session, sql: &str) -> Vec<String> { s.exec_s(sess, sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}")).iter().map(|r| r.iter().map(|v| format!("{v:?}")).collect::<Vec<_>>().join(",")).collect() }
    fn as_app(s: &Sql, sql: &str) { s.exec_s(&mut Session::default(), sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}")); }
    fn attrs(kv: &[(&str, Value)]) -> HashMap<String, Value> { kv.iter().map(|(k, v)| (k.to_string(), v.clone())).collect() }
    #[test] fn an_app_supplied_identity_gets_its_roles_and_attributes_and_nothing_more() {
        let d = std::env::temp_dir().join(format!("sqweezdb-identity-{}", std::process::id())); let _ = std::fs::remove_dir_all(&d);
        let s = Sql::open_with(&d, Config::default()).unwrap();
        as_app(&s, "CREATE TABLE orders (id SERIAL PRIMARY KEY, tenant_id INT NOT NULL, item VARCHAR(32))");
        as_app(&s, "INSERT INTO orders (tenant_id, item) VALUES (1, 'apples'), (1, 'pears'), (2, 'bolts')");
        as_app(&s, "CREATE ROLE customer"); as_app(&s, "CREATE ROLE auditor");
        as_app(&s, "GRANT SELECT, INSERT ON orders TO customer"); as_app(&s, "GRANT SELECT ON orders TO auditor");
        as_app(&s, "CREATE POLICY own_orders ON orders FOR ALL TO customer USING (tenant_id = SESSION.tenant)");
        let customer = |t: i64| { let mut x = Session::default(); s.assume(&mut x, &["customer".into()], attrs(&[("tenant", Value::Int(t))])).unwrap(); x };
        // reads: each tenant its own rows, even asking by id
        assert_eq!(rows(&s, &mut customer(1), "SELECT item FROM orders ORDER BY item"), vec!["Text(\"apples\")", "Text(\"pears\")"]);
        assert_eq!(rows(&s, &mut customer(2), "SELECT item FROM orders"), vec!["Text(\"bolts\")"]);
        assert!(rows(&s, &mut customer(1), "SELECT item FROM orders WHERE id = 3").is_empty());
        // writes: inside the policy only
        let mut c1 = customer(1);
        s.exec_s(&mut c1, "INSERT INTO orders (tenant_id, item) VALUES (1, 'plums')", &[]).unwrap();
        let e = s.exec_s(&mut c1, "INSERT INTO orders (tenant_id, item) VALUES (2, 'sneaky')", &[]).unwrap_err();
        assert!(e.starts_with("permission denied") && e.contains("policy"), "{e}");
        let e = s.exec_s(&mut c1, "DELETE FROM orders WHERE id = 1", &[]).unwrap_err();
        assert_eq!(e, "permission denied: role customer lacks DELETE on orders");
        // an identity is never the application: no schema or access changes
        assert!(s.exec_s(&mut c1, "CREATE TABLE x (id INT PRIMARY KEY)", &[]).unwrap_err().starts_with("permission denied"));
        assert!(s.exec_s(&mut c1, "CREATE ROLE boss", &[]).unwrap_err().starts_with("permission denied"));
        // a role without a policy sees every row it is granted
        let mut a = Session::default(); s.assume(&mut a, &["auditor".into()], HashMap::new()).unwrap();
        assert_eq!(rows(&s, &mut a, "SELECT count(*) FROM orders"), vec!["Int(4)"]);
        // refused: a missing attribute (fails closed), an unknown role, no role, an attribute without a value, mid-transaction
        let mut none = Session::default(); s.assume(&mut none, &["customer".into()], HashMap::new()).unwrap();
        assert_eq!(s.exec_s(&mut none, "SELECT * FROM orders", &[]).unwrap_err(), "policy own_orders needs session attribute tenant");
        assert_eq!(s.assume(&mut Session::default(), &["admin".into()], HashMap::new()).unwrap_err(), "no such role admin");
        assert_eq!(s.assume(&mut Session::default(), &[], HashMap::new()).unwrap_err(), "an identity needs at least one role");
        assert_eq!(s.assume(&mut Session::default(), &["customer".into()], attrs(&[("tenant", Value::Null)])).unwrap_err(), "session attribute tenant has no value");
        let mut t = customer(1); s.exec_s(&mut t, "BEGIN", &[]).unwrap();
        assert_eq!(s.assume(&mut t, &["customer".into()], attrs(&[("tenant", Value::Int(2))])).unwrap_err(), "cannot change identity inside a transaction");
        // the JSON form (C ABI, Go): numbers, text, booleans; a role as a string or a list
        let mut j = Session::default(); s.assume_json(&mut j, r#"{"roles": "customer", "attrs": {"tenant": 2, "name": "bob", "vip": true, "score": 1.5}}"#).unwrap();
        assert_eq!((j.attrs["tenant"].clone(), j.attrs["name"].clone(), j.attrs["vip"].clone(), j.attrs["score"].clone()), (Value::Int(2), Value::Text("bob".into()), Value::Bool(true), Value::Float(1.5)));
        assert_eq!(rows(&s, &mut j, "SELECT item FROM orders"), vec!["Text(\"bolts\")"]);
        assert!(s.assume_json(&mut Session::default(), r#"{"roles": ["customer"], "attrs": {"tenant": [1]}}"#).unwrap_err().contains("must be a number, text or true/false"));
        assert_eq!(s.assume_json(&mut Session::default(), r#"{"roles": ["nobody"]}"#).unwrap_err(), "no such role nobody");
        drop(s); std::fs::remove_dir_all(&d).unwrap();
    }
    #[test] fn a_uuid_attribute_scopes_a_uuid_keyed_table() {
        let d = std::env::temp_dir().join(format!("sqweezdb-identity-uuid-{}", std::process::id())); let _ = std::fs::remove_dir_all(&d);
        let s = Sql::open_with(&d, Config::default()).unwrap();
        let (a, b) = ("01a14620-70cb-7000-9087-223339ad636c", "01a14620-70cb-7001-be11-62ba95687997");
        as_app(&s, "CREATE TABLE notes (id UUID PRIMARY KEY DEFAULT uuid(), owner UUID NOT NULL, body VARCHAR(32))");
        as_app(&s, &format!("INSERT INTO notes (owner, body) VALUES ('{a}', 'mine'), ('{b}', 'theirs')"));
        as_app(&s, "CREATE ROLE member"); as_app(&s, "GRANT SELECT ON notes TO member");
        as_app(&s, "CREATE POLICY own_notes ON notes FOR SELECT TO member USING (owner = SESSION.user_id)");
        let mut m = Session::default(); s.assume(&mut m, &["member".into()], attrs(&[("user_id", Value::Text(a.into()))])).unwrap();
        assert_eq!(rows(&s, &mut m, "SELECT body FROM notes"), vec!["Text(\"mine\")"]);
        let mut bad = Session::default(); s.assume(&mut bad, &["member".into()], attrs(&[("user_id", Value::Text("nope".into()))])).unwrap();
        assert!(s.exec_s(&mut bad, "SELECT body FROM notes", &[]).unwrap_err().contains("not a UUID"));
        drop(s); std::fs::remove_dir_all(&d).unwrap();
    }
}
