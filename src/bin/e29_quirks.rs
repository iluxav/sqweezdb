//! Experiment 29: SQLite's own "Quirks" page (sqlite.org/quirks.html), one statement sequence per quirk, run against
//! SQLite (system lib, in memory) and ours. Expected verdicts are stated up front and checked.
use sqweezdb::sql::{Session, Sql};
use sqweezdb::table::Value;
use rusqlite::{types::Value as SV, Connection};
use std::path::PathBuf;

struct Check { name: &'static str, sqlite: Vec<String>, ours: Vec<String>, param: Option<String>, expect: &'static str, note: &'static str }
fn same(n: &'static str, stmts: &[&str], expect: &'static str, note: &'static str) -> Check { Check { name: n, sqlite: stmts.iter().map(|s| s.to_string()).collect(), ours: stmts.iter().map(|s| s.to_string()).collect(), param: None, expect, note } }
fn fmt_sv(v: &SV) -> String { match v { SV::Null => "NULL".into(), SV::Integer(i) => i.to_string(), SV::Real(f) => format!("{f}"), SV::Text(s) => format!("'{}'", if s.len() > 20 { format!("{}…({} chars)", &s[..10], s.len()) } else { s.replace('\0', "\\0") }), SV::Blob(b) => format!("blob({})", b.len()) } }
fn fmt_ov(v: &Value) -> String { match v { Value::Null => "NULL".into(), Value::Int(i) => i.to_string(), Value::Float(f) => format!("{f}"), Value::Bool(b) => b.to_string(), Value::Text(s) => format!("'{}'", if s.len() > 20 { format!("{}…({} chars)", &s[..10], s.len()) } else { s.replace('\0', "\\0") }), Value::Bytes(b) => format!("blob({})", b.len()) } }
fn sq(c: &Connection, stmt: &str, param: &Option<String>) -> Result<Vec<Vec<String>>, String> {
    let mut st = c.prepare(stmt).map_err(|e| e.to_string())?;
    let p: Vec<&dyn rusqlite::ToSql> = param.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
    if st.column_count() == 0 { st.execute(&p[..]).map_err(|e| e.to_string())?; return Ok(vec![]); }
    let n = st.column_count(); let rows = st.query_map(&p[..], |r| Ok((0..n).map(|i| fmt_sv(&r.get::<_, SV>(i).unwrap())).collect::<Vec<_>>())).map_err(|e| e.to_string())?;
    rows.collect::<Result<Vec<_>, _>>().map_err(|e| e.to_string())
}
fn ou(sql: &Sql, s: &mut Session, stmt: &str, param: &Option<String>) -> Result<Vec<Vec<String>>, String> {
    let p: Vec<Value> = param.iter().map(|x| Value::Text(x.clone())).collect();
    Ok(sql.exec_s(s, stmt, &p)?.iter().map(|r| r.iter().map(fmt_ov).collect()).collect())
}
fn outcome(r: &Result<Vec<Vec<String>>, String>) -> String { match r { Ok(rows) if rows.is_empty() => "ok".into(), Ok(rows) => format!("ok: {}", rows.iter().take(3).map(|r| r.join("|")).collect::<Vec<_>>().join(" ; ")), Err(e) => format!("error: {}", e.chars().take(90).collect::<String>()) } }

fn main() {
    let c = Connection::open_in_memory().unwrap(); let dir = PathBuf::from("data/nocow/e29"); let sql = Sql::recreate(&dir, false); let mut s = Session::default();
    let long = "x".repeat(2000); let nul = "a\0b".to_string();
    let seed = ["CREATE TABLE q(id INTEGER PRIMARY KEY, score DOUBLE, country INT, name VARCHAR(32))", "INSERT INTO q VALUES (1, 10.5, 1, 'ann')", "INSERT INTO q VALUES (2, 99.0, 1, 'bob')", "INSERT INTO q VALUES (3, 50.0, 2, 'cid')"];
    for st in seed { sq(&c, st, &None).unwrap(); ou(&sql, &mut s, st, &None).unwrap(); }
    let mut checks = vec![
        same("text stored in an INT column", &["CREATE TABLE q1(id INTEGER PRIMARY KEY, n INT)", "INSERT INTO q1 VALUES (1, 'wxyz')", "SELECT n FROM q1 WHERE id = 1"], "ours rejects", "SQLite keeps 'wxyz' in an INT column; ours fails the INSERT with the column and both types"),
        Check { name: "2000 characters into VARCHAR(50)", sqlite: vec!["CREATE TABLE q2(id INTEGER PRIMARY KEY, s VARCHAR(50))".into(), "INSERT INTO q2 VALUES (1, ?)".into(), "SELECT s FROM q2".into()], ours: vec!["CREATE TABLE q2(id INTEGER PRIMARY KEY, s VARCHAR(50))".into(), "INSERT INTO q2 VALUES (1, ?)".into(), "SELECT s FROM q2".into()], param: Some(long.clone()), expect: "ours rejects", note: "SQLite stores all 2000; ours fails the INSERT (value exceeds VARCHAR(50))" },
        Check { name: "the same with SQLite's opt-in STRICT table", sqlite: vec!["CREATE TABLE q3(id INTEGER PRIMARY KEY, n INT) STRICT".into(), "INSERT INTO q3 VALUES (1, 'wxyz')".into()], ours: vec!["CREATE TABLE q3(id INTEGER PRIMARY KEY, n INT)".into(), "INSERT INTO q3 VALUES (1, 'wxyz')".into()], param: None, expect: "both reject", note: "SQLite 3.37+ can be strict per table if asked; ours always is" },
        same("no BOOLEAN type: text into a BOOLEAN column", &["CREATE TABLE q4(id INTEGER PRIMARY KEY, f BOOLEAN)", "INSERT INTO q4 VALUES (1, 'yes')", "SELECT f FROM q4"], "ours rejects", "SQLite stores 'yes'; ours has a real BOOL type"),
        same("TRUE/FALSE become column names", &["CREATE TABLE q5(id INTEGER PRIMARY KEY, true INT)", "INSERT INTO q5 VALUES (1, 0)", "SELECT true FROM q5"], "ours rejects", "in SQLite 'SELECT true' returns the column (0), not the boolean"),
        same("DATETIME column accepts anything", &["CREATE TABLE q6(id INTEGER PRIMARY KEY, at DATETIME)", "INSERT INTO q6 VALUES (1, 'not a date')", "SELECT at FROM q6"], "ours rejects", "neither engine has a date type; SQLite silently accepts the declaration and any value, ours refuses the declaration (a TIMESTAMP type is H40)"),
        same("columns declared without any type", &["CREATE TABLE q7(a, b, c)", "INSERT INTO q7 VALUES (1, 'x', 2.5)", "SELECT a, b, c FROM q7"], "ours rejects", ""),
        same("foreign key not enforced", &["CREATE TABLE q8p(id INTEGER PRIMARY KEY)", "CREATE TABLE q8c(id INTEGER PRIMARY KEY, pid INT REFERENCES q8p(id))", "INSERT INTO q8c VALUES (1, 999)", "SELECT pid FROM q8c"], "same", "SQLite parses REFERENCES and ignores it unless PRAGMA foreign_keys is on for the connection; ours does not enforce foreign keys yet either (planned round)"),
        same("NULL in a PRIMARY KEY (INT PRIMARY KEY is not the rowid)", &["CREATE TABLE q9(a INT PRIMARY KEY, b INT)", "INSERT INTO q9 VALUES (NULL, 1)", "SELECT a, b FROM q9"], "ours rejects", "a documented SQLite bug kept for compatibility"),
        same("NULL into INTEGER PRIMARY KEY is auto-assigned", &["CREATE TABLE q10(id INTEGER PRIMARY KEY, b INT)", "INSERT INTO q10 VALUES (NULL, 1)", "SELECT id, b FROM q10"], "ours rejects", "SQLite assigns 1; ours refuses a NULL key (the one E19a difference)"),
        same("bare column next to max()", &["SELECT id, max(score) FROM q"], "ours rejects", "SQLite returns the id of the max row (arbitrary on ties); ours requires a GROUP BY. Before this round ours silently returned NULL for the bare column: a bug found by this checklist"),
        same("GROUP BY without an aggregate picks an arbitrary row", &["SELECT country, id FROM q GROUP BY country"], "ours rejects", "before this round ours returned every row ungrouped: a bug found by this checklist"),
        same("double-quoted string becomes a literal when no such column exists", &["SELECT id FROM q WHERE name = \"nosuch\""], "ours rejects", "SQLite returns 0 rows silently: a misspelled identifier becomes a string; ours reports the unknown column"),
        same("keywords as table and column names", &["CREATE TABLE union(id INTEGER PRIMARY KEY, with BOOLEAN)", "INSERT INTO union VALUES (1, TRUE)", "SELECT with FROM union"], "sqlite rejects", "the quirks page's own example; SQLite 3.53 now rejects 'union' as a table name, our parser accepts both names: not a quirk we avoid"),
        same("CREATE TABLE t(INTEGER PRIMARY KEY) makes a column named INTEGER", &["CREATE TABLE q15(INTEGER PRIMARY KEY)", "INSERT INTO q15 VALUES ('x')", "SELECT * FROM q15"], "ours rejects", ""),
        Check { name: "NUL byte inside text survives", sqlite: vec!["CREATE TABLE q16(id INTEGER PRIMARY KEY, s VARCHAR(10))".into(), "INSERT INTO q16 VALUES (1, ?)".into(), "SELECT s FROM q16".into()], ours: vec!["CREATE TABLE q16(id INTEGER PRIMARY KEY, s VARCHAR(10))".into(), "INSERT INTO q16 VALUES (1, ?)".into(), "SELECT s FROM q16".into()], param: Some(nul.clone()), expect: "same", note: "both store it; SQLite's own string functions stop at the NUL (we have none to test)" },
        same("'1' matches an integer key through type affinity", &["SELECT id FROM q WHERE id = '1'"], "ours rejects", "SQLite converts '1' to 1 because the column is INTEGER, while 1='1' as an expression is false; ours refuses to compare text with an INT column"),
        same("a double in a TEXT comparison", &["SELECT id FROM q WHERE name = 1.5"], "ours rejects", "SQLite compares 'ann' with 1.5 and returns nothing; ours reports the type mismatch"),
    ];
    let not_built = ["CREATE TRIGGER AFTER INSERT ON t ... creates a trigger named AFTER (no triggers in ours)", "upper()/lower() only fold ASCII (no string functions in ours)", "comma joins vs RIGHT/FULL JOIN precedence (no RIGHT/FULL JOIN in ours)", "AUTOINCREMENT differs from MySQL (no AUTOINCREMENT in ours)", "WITHOUT ROWID tables (no rowid concept in ours)", "ALTER TABLE limits (ALTER is the next round)"];
    println!("=== E29: SQLite's documented quirks, same statements both sides ===");
    let (mut rejects, mut sames, mut both, mut diff, mut mism) = (0, 0, 0, 0, 0);
    for (i, ch) in checks.iter_mut().enumerate() {
        let mut rs: Result<Vec<Vec<String>>, String> = Ok(vec![]); for st in &ch.sqlite { rs = sq(&c, st, &if st.contains('?') { ch.param.clone() } else { None }); if rs.is_err() { break; } }
        let mut ro: Result<Vec<Vec<String>>, String> = Ok(vec![]); for st in &ch.ours { ro = ou(&sql, &mut s, st, &if st.contains('?') { ch.param.clone() } else { None }); if ro.is_err() { break; } }
        let verdict = match (&rs, &ro) { (Ok(_), Err(_)) => "ours rejects", (Ok(a), Ok(b)) if a == b => "same", (Ok(_), Ok(_)) => "different", (Err(_), Err(_)) => "both reject", (Err(_), Ok(_)) => "sqlite rejects" };
        match verdict { "ours rejects" => rejects += 1, "same" => sames += 1, "both reject" => both += 1, _ => diff += 1 }
        let ok = verdict == ch.expect; if !ok { mism += 1; }
        println!("{:>2}. {}\n    sqlite: {}\n    ours:   {}\n    -> {verdict}{}{}", i + 1, ch.name, outcome(&rs), outcome(&ro), if ok { "" } else { "  <-- EXPECTED " }, if ok { String::new() } else { ch.expect.to_string() });
        if !ch.note.is_empty() { println!("    note: {}", ch.note); }
    }
    for n in not_built { println!("    not built in ours: {n}"); }
    println!("summary: {} checks: ours rejects {rejects}, same {sames}, both reject {both}, different {diff}; {mism} differ from the stated expectation; {} quirks concern features ours does not have", checks.len(), not_built.len());
    drop(sql); let _ = std::fs::remove_dir_all(&dir);
}
