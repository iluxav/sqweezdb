//! The web console (`listen.http`): one page compiled into the binary plus a small JSON API over HTTP/1.1, served by
//! the owning process. The page is public; every `/api/` call needs the listener's token (`Authorization: Bearer`,
//! or `?token=` for the event streams a browser cannot set headers on). Bind it to loopback unless you mean otherwise.
//! The console runs as the application session: it sees everything, like the application does.
use crate::live::Event;
use crate::sql::{ChangeOp, PolicyRhs, Session, Sql, P_DELETE, P_INSERT, P_SELECT, P_UPDATE};
use crate::table::{ColType, Value};
use serde_json::{json, Value as J};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub const PAGE: &str = include_str!("ui/index.html");
const MAX_BODY: usize = 1 << 20;
type Cols = Vec<(String, Option<ColType>)>;

pub fn start(sql: Arc<Sql>, addr: &str, token: &str) -> Result<crate::net::Listener, String> {
    if token.is_empty() { return Err("listen.http needs listen.token".into()); }
    let listener = TcpListener::bind(addr).map_err(|e| format!("listen.http {addr}: {e}"))?;
    let weak = Arc::downgrade(&sql); drop(sql); let token: Arc<str> = Arc::from(token);
    crate::net::accept_tcp(listener, move |stream| {
        if weak.strong_count() == 0 { return false; }
        stream.set_nodelay(true).ok();
        let (w, t) = (weak.clone(), token.clone());
        std::thread::spawn(move || { let _ = serve(w, stream, &t); });
        true
    }).map_err(|e| format!("listen.http {addr}: {e}"))
}

struct Req { method: String, path: String, query: HashMap<String, String>, headers: HashMap<String, String>, body: Vec<u8> }
fn read_req(r: &mut BufReader<TcpStream>) -> std::io::Result<Option<Req>> {
    let mut line = String::new(); if r.read_line(&mut line)? == 0 { return Ok(None); }
    let mut parts = line.split_whitespace(); let method = parts.next().unwrap_or("").to_string(); let target = parts.next().unwrap_or("/").to_string();
    let mut headers = HashMap::new();
    loop { let mut h = String::new(); if r.read_line(&mut h)? == 0 { return Ok(None); } let h = h.trim_end(); if h.is_empty() { break; } if let Some((k, v)) = h.split_once(':') { headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string()); } }
    let len: usize = headers.get("content-length").and_then(|v| v.parse().ok()).unwrap_or(0);
    if len > MAX_BODY { return Err(std::io::Error::other("body too large")); }
    let mut body = vec![0u8; len]; if len > 0 { r.read_exact(&mut body)?; }
    let (path, q) = match target.split_once('?') { Some((p, q)) => (p.to_string(), q.to_string()), None => (target.clone(), String::new()) };
    let query = q.split('&').filter(|s| !s.is_empty()).map(|kv| { let (k, v) = kv.split_once('=').unwrap_or((kv, "")); (url_decode(k), url_decode(v)) }).collect();
    Ok(Some(Req { method, path, query, headers, body }))
}
fn url_decode(s: &str) -> String {
    let b = s.as_bytes(); let mut out = Vec::with_capacity(b.len()); let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() { if let Ok(v) = u8::from_str_radix(std::str::from_utf8(&b[i + 1..i + 3]).unwrap_or("zz"), 16) { out.push(v); i += 3; continue; } }
        out.push(if b[i] == b'+' { b' ' } else { b[i] }); i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}
fn respond(w: &mut TcpStream, status: u16, ctype: &str, body: &[u8]) -> std::io::Result<()> {
    let reason = match status { 200 => "OK", 400 => "Bad Request", 401 => "Unauthorized", 404 => "Not Found", 405 => "Method Not Allowed", _ => "Error" };
    // one write per response: a response written in fragments waits on the peer's delayed ACK for each one
    let mut out = format!("HTTP/1.1 {status} {reason}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nX-Frame-Options: DENY\r\nContent-Security-Policy: default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; img-src data:\r\nConnection: keep-alive\r\n\r\n", body.len()).into_bytes();
    out.extend_from_slice(body); w.write_all(&out)?; w.flush()
}
fn json_resp(w: &mut TcpStream, status: u16, v: &J) -> std::io::Result<()> { respond(w, status, "application/json", v.to_string().as_bytes()) }
fn authorized(req: &Req, token: &str) -> bool {
    let given = req.headers.get("authorization").and_then(|h| h.strip_prefix("Bearer ")).map(|s| s.trim().to_string()).or_else(|| req.query.get("token").cloned());
    given.map(|g| crate::net::same_secret(g.as_bytes(), token.as_bytes())).unwrap_or(false)
}

fn serve(weak: std::sync::Weak<Sql>, stream: TcpStream, token: &str) -> std::io::Result<()> {
    let mut r = BufReader::with_capacity(1 << 16, stream.try_clone()?); let mut w = stream;
    let mut sess = Session::default();
    loop {
        let Some(req) = read_req(&mut r)? else { return Ok(()) };
        let Some(sql) = weak.upgrade() else { return Ok(()) };   // held for this request only
        let close = req.headers.get("connection").map(|c| c.eq_ignore_ascii_case("close")).unwrap_or(false);
        match (req.method.as_str(), req.path.as_str()) {
            ("GET", "/") | ("GET", "/index.html") => respond(&mut w, 200, "text/html; charset=utf-8", PAGE.as_bytes())?,
            (_, p) if p.starts_with("/api/") => {
                if !authorized(&req, token) { json_resp(&mut w, 401, &json!({"error": "token required", "code": "DENIED"}))?; }
                else {
                    match (req.method.as_str(), p) {
                        ("POST", "/api/sql") => { let v = api_sql(&sql, &mut sess, &req.body); json_resp(&mut w, if v.get("error").is_some() { 400 } else { 200 }, &v)? }
                        ("GET", "/api/tables") => json_resp(&mut w, 200, &api_tables(&sql))?,
                        ("GET", "/api/status") => json_resp(&mut w, 200, &api_status(&sql))?,
                        ("GET", "/api/acl") => json_resp(&mut w, 200, &api_acl(&sql))?,
                        ("POST", "/api/snapshot") => { let st = sql.snapshot(); json_resp(&mut w, 200, &json!({"rows": st.rows, "bytes": st.bytes, "secs": st.secs, "deletedSegments": st.deleted_segments}))? }
                        ("GET", "/api/live") => { drop(sql); return stream_events(&weak, &sess, &req, w, false) }
                        ("GET", "/api/changes") => { drop(sql); return stream_events(&weak, &sess, &req, w, true) }
                        _ => json_resp(&mut w, 404, &json!({"error": "no such endpoint", "code": "ERROR"}))?,
                    }
                }
            }
            _ => respond(&mut w, 404, "text/plain", b"not found")?,
        }
        if close { return Ok(()); }
    }
}

fn err_json(e: String) -> J { json!({"error": e, "code": crate::sql::error_code(&e)}) }
fn j_to_value(v: &J) -> Value { match v { J::Null => Value::Null, J::Bool(b) => Value::Bool(*b), J::Number(n) => match n.as_i64() { Some(i) => Value::Int(i), None => Value::Float(n.as_f64().unwrap_or(0.0)) }, J::String(s) => Value::Text(s.clone()), other => Value::Text(other.to_string()) } }
fn value_json(v: &Value, ty: Option<&ColType>) -> J {
    match v { Value::Null => J::Null, Value::Bool(b) => json!(b), Value::Int(i) => if matches!(ty, Some(ColType::Timestamp)) { json!(crate::table::format_timestamp(*i)) } else { json!(i) }, Value::Float(f) => json!(f), Value::Text(s) => json!(s), Value::Bytes(b) => json!(format!("0x{}", b.iter().map(|x| format!("{x:02x}")).collect::<String>())) }
}
fn row_json(cols: &Cols, r: &[Value]) -> J { J::Array(r.iter().enumerate().map(|(i, v)| value_json(v, cols.get(i).and_then(|c| c.1.as_ref()))).collect()) }
fn names(cols: &Cols) -> Vec<&str> { cols.iter().map(|c| c.0.as_str()).collect() }

fn api_sql(sql: &Sql, sess: &mut Session, body: &[u8]) -> J {
    let v: J = match serde_json::from_slice(body) { Ok(v) => v, Err(e) => return json!({"error": format!("bad JSON: {e}"), "code": "ARGUMENTS"}) };
    let text = v.get("sql").and_then(|s| s.as_str()).unwrap_or("").trim().trim_end_matches(';').to_string();
    if text.is_empty() { return json!({"error": "sql is required", "code": "ARGUMENTS"}); }
    let params: Vec<Value> = v.get("params").and_then(|p| p.as_array()).map(|a| a.iter().map(j_to_value).collect()).unwrap_or_default();
    let t = Instant::now();
    match sql.query(sess, &text, &params) {
        Ok((cols, rows)) => json!({ "columns": names(&cols), "types": cols.iter().map(|(_, t)| t.as_ref().map(|t| t.name())).collect::<Vec<_>>(), "rows": rows.iter().map(|r| row_json(&cols, r)).collect::<Vec<_>>(), "count": rows.len(), "ms": t.elapsed().as_secs_f64() * 1e3, "inTransaction": sess.txn.is_some() }),
        Err(e) => err_json(e),
    }
}
fn api_tables(sql: &Sql) -> J {
    let mut sess = Session::default();
    J::Array(sql.db.table_names().iter().filter_map(|n| {
        let t = sql.db.table(n)?;
        let rows = sql.exec_s(&mut sess, &format!("SELECT count(*) FROM {n}"), &[]).ok().and_then(|r| r.first().and_then(|r| r.first()).and_then(|v| if let Value::Int(i) = v { Some(*i) } else { None })).unwrap_or(-1);
        Some(json!({ "name": t.schema.name, "rows": rows, "columns": t.schema.columns.iter().enumerate().map(|(i, c)| json!({"name": c.name, "type": c.ty.name(), "nullable": c.nullable, "pk": i == t.schema.pk, "unique": t.schema.unique == Some(i)})).collect::<Vec<_>>(), "foreignKeys": t.schema.fks.len() }))
    }).collect())
}
fn api_status(sql: &Sql) -> J {
    use crate::config::Durability;
    let (seq, durable, bytes) = { let s = sql.db.log.st.lock().unwrap(); (s.seq, s.durable, s.bytes) };
    let cfg = &sql.cfg;
    let backup = sql.backup_stats().map(|st| json!({ "bucket": cfg.backup.as_ref().map(|b| format!("{}/{}", b.bucket, b.prefix.trim_matches('/'))), "endpoint": cfg.backup.as_ref().map(|b| b.endpoint.clone()), "objects": st.puts.load(Relaxed), "bytes": st.bytes.load(Relaxed), "cloudSeq": st.cloud_seq.load(Relaxed), "behind": seq.saturating_sub(st.cloud_seq.load(Relaxed)), "snapshots": st.snapshots.load(Relaxed), "encrypted": cfg.backup.as_ref().map(|b| b.passphrase.is_some()).unwrap_or(false) }));
    json!({ "dir": sql.db.dir.display().to_string(), "durable": match cfg.durable { Durability::Visible => "visible", Durability::Fsync => "fsync", Durability::Cloud => "cloud" }, "seq": seq, "durableSeq": durable, "logBytes": bytes,
            "snapshotsByPolicy": sql.snapshots_taken(), "snapshotPolicy": {"logBytes": cfg.snapshot.log_bytes, "idleAfterMs": cfg.snapshot.idle_after.map(|d| d.as_millis() as u64)},
            "altersInProgress": sql.db.tables_with_shadow().iter().map(|(n, _)| n.clone()).collect::<Vec<_>>(),
            "listen": {"socket": cfg.listen.socket.as_ref().map(|p| p.display().to_string()), "tcp": cfg.listen.tcp, "http": cfg.listen.http},
            "backup": backup, "followerApplied": sql.follower_applied(), "tables": sql.db.table_names().len(), "reactor": sql.live.get().map(|r| json!({"events": r.events.load(Relaxed), "reruns": r.reruns.load(Relaxed)})) })
}
fn cmp_sym(c: crate::sql::Cmp) -> &'static str { use crate::sql::Cmp::*; match c { Eq => "=", Ne => "<>", Lt => "<", Le => "<=", Gt => ">", Ge => ">=", IsNull => "IS NULL", IsNotNull => "IS NOT NULL", In => "IN", NotIn => "NOT IN" } }
fn cmds_str(m: u8) -> Vec<&'static str> { let mut v = vec![]; if m & P_SELECT != 0 { v.push("SELECT"); } if m & P_INSERT != 0 { v.push("INSERT"); } if m & P_UPDATE != 0 { v.push("UPDATE"); } if m & P_DELETE != 0 { v.push("DELETE"); } v }
fn api_acl(sql: &Sql) -> J {
    let a = sql.acl.read().unwrap();
    let mut roles: Vec<String> = a.roles.iter().cloned().collect(); roles.sort();
    let mut users: Vec<J> = a.users.iter().map(|(u, r)| json!({"name": u, "roles": r.roles, "attrs": r.attrs.iter().map(|(k, v)| (k.clone(), value_json(v, None))).collect::<serde_json::Map<_, _>>()})).collect();
    users.sort_by_key(|x| x["name"].as_str().unwrap_or("").to_string());
    json!({
        "roles": roles,
        "users": users,
        "grants": a.grants.iter().map(|((role, table), m)| json!({"role": role, "table": table, "commands": cmds_str(*m)})).collect::<Vec<_>>(),
        "policies": a.policies.iter().map(|p| json!({"name": p.name, "table": p.table, "role": p.role, "commands": cmds_str(p.cmds), "using": p.preds.iter().map(|(c, cmp, rhs)| format!("{c} {} {}", cmp_sym(*cmp), match rhs { PolicyRhs::Lit(v) => value_json(v, None).to_string(), PolicyRhs::Attr(n) => format!("SESSION.{n}") })).collect::<Vec<_>>().join(" AND ")})).collect::<Vec<_>>(),
    })
}
/// Server-Sent Events: a live query (`/api/live`, the whole result on every change) or a change tail (`/api/changes`).
fn stream_events(weak: &std::sync::Weak<Sql>, sess: &Session, req: &Req, mut w: TcpStream, changes: bool) -> std::io::Result<()> {
    let Some(sql) = weak.upgrade() else { return Ok(()) };
    let text = req.query.get("sql").cloned().unwrap_or_default();
    let params: Vec<Value> = req.query.get("params").and_then(|p| serde_json::from_str::<J>(p).ok()).and_then(|v| v.as_array().map(|a| a.iter().map(j_to_value).collect())).unwrap_or_default();
    let cols: Cols = sql.prepare(&text).map(|p| sql.columns(&p)).unwrap_or_default();
    let (sub, initial) = if changes { match sql.reactor().subscribe(sess, &text, &params) { Ok(s) => (s, None), Err(e) => return json_resp(&mut w, 400, &err_json(e)) } }
                         else { match sql.reactor().watch(sess, &text, &params) { Ok((s, rows)) => (s, Some(rows)), Err(e) => return json_resp(&mut w, 400, &err_json(e)) } };
    w.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\n\r\n")?;
    let send = |w: &mut TcpStream, v: J| -> std::io::Result<()> { w.write_all(format!("data: {v}\n\n").as_bytes())?; w.flush() };
    if let Some(rows) = initial { send(&mut w, json!({"type": "rows", "columns": names(&cols), "rows": rows.iter().map(|r| row_json(&cols, r)).collect::<Vec<_>>(), "initial": true}))?; }
    drop(sql);   // a stream must not keep the database open
    let mut table_cols: HashMap<String, Cols> = HashMap::new(); let mut quiet = Instant::now();
    loop {
        let ev = sub.recv_timeout(Duration::from_secs(1));
        if weak.strong_count() == 0 { return Ok(()); }
        if ev.is_some() { quiet = Instant::now(); }
        match ev {
            Some(Event::Live { seq, rows, added, removed }) => send(&mut w, json!({"type": "rows", "seq": seq, "columns": names(&cols), "rows": rows.iter().map(|r| row_json(&cols, r)).collect::<Vec<_>>(), "added": added.len(), "removed": removed.len()}))?,
            Some(Event::Change(c)) => {
                let tc = table_cols.entry(c.table.clone()).or_insert_with(|| weak.upgrade().and_then(|s| s.db.table(&c.table)).map(|t| t.schema.columns.iter().map(|col| (col.name.clone(), Some(col.ty.clone()))).collect()).unwrap_or_default());
                send(&mut w, json!({"type": "change", "op": match c.op { ChangeOp::Insert => "insert", ChangeOp::Update => "update", ChangeOp::Delete => "delete" }, "table": c.table, "key": c.pk, "seq": c.seq, "columns": names(tc), "row": c.row.as_ref().map(|r| row_json(tc, r))}))?
            }
            Some(Event::Overflow { last_seq }) => { send(&mut w, json!({"type": "overflow", "seq": last_seq}))?; return Ok(()); }
            None => if quiet.elapsed() >= Duration::from_secs(15) { w.write_all(b": keepalive\n\n")?; w.flush()?; quiet = Instant::now(); },
        }
    }
}
