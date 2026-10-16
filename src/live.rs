//! Reactive layer (E37): filtered change subscriptions with the subscriber's row policies applied to every event, and
//! live queries kept fresh by invalidation. One reactor thread per database reads the in-process change ring, evaluates
//! compiled predicates on the raw row bytes, and pushes events into bounded per-subscriber queues. Writers never wait.
use crate::sql::{Access, Change, ChangeOp, Cmp, CPred, Expr, Plan, Pred, Prepared, Proj, SelectPlan, Session, Sql, P_SELECT};
use crate::table::{decode_col, read_key, Value, OP_DELETE, OP_INSERT, OP_UPDATE};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub enum Event {
    /// One decoded change, shared by every subscriber it reaches.
    Change(Arc<Change>),
    /// A watched query's result changed: the whole new result, plus the rows that appeared and disappeared (by first column when it is the key).
    Live { seq: u64, rows: Vec<Vec<Value>>, added: Vec<Vec<Value>>, removed: Vec<Vec<Value>> },
    /// The subscriber fell more than `cap` events behind and was dropped; resubscribe (a feed from `last_seq` recovers everything).
    Overflow { last_seq: u64 },
}
pub struct Queue { items: Mutex<VecDeque<Event>>, cv: Condvar, cap: usize, pub closed: AtomicBool, pub delivered: AtomicU64 }
impl Queue {
    fn push(&self, e: Event) -> bool { let mut q = self.items.lock().unwrap(); if q.len() >= self.cap { return false; } q.push_back(e); self.delivered.fetch_add(1, Ordering::Relaxed); self.cv.notify_one(); true }
}
/// A subscriber's handle: pop events; dropping it unsubscribes.
pub struct Subscription { pub q: Arc<Queue> }
impl Subscription {
    pub fn try_recv(&self) -> Option<Event> { self.q.items.lock().unwrap().pop_front() }
    pub fn recv_timeout(&self, d: Duration) -> Option<Event> { let mut q = self.q.items.lock().unwrap(); if q.is_empty() { let (g, _) = self.q.cv.wait_timeout(q, d).unwrap(); q = g; } q.pop_front() }
    pub fn pending(&self) -> usize { self.q.items.lock().unwrap().len() }
}
impl Drop for Subscription { fn drop(&mut self) { self.q.closed.store(true, Ordering::Relaxed); } }

struct Sub { table: u16, preds: Vec<CPred>, policies: Vec<CPred>, q: Arc<Queue>, key: Option<(usize, i64)> }
struct Live { prepared: Arc<Prepared>, sess: Session, params: Vec<Value>, tables: Vec<u16>, single: Option<(u16, Vec<CPred>, Vec<CPred>)>, key_col: Option<usize>, result: Vec<Vec<Value>>, keys: HashSet<i64>, dirty_since: Option<Instant>, q: Arc<Queue> }
/// Subscriptions whose filter is `col = constant` on an INT column are indexed by (table, column, value): a record
/// reaches them through one hash lookup per indexed column instead of a scan over every subscriber.
#[derive(Default)] struct State { subs: Vec<Sub>, lives: Vec<Live>, keyed: HashMap<(u16, usize, i64), Vec<usize>>, key_cols: HashMap<u16, Vec<usize>>, general: HashMap<u16, Vec<usize>> }
impl State {
    fn reindex(&mut self) { self.keyed.clear(); self.key_cols.clear(); self.general.clear(); for (i, s) in self.subs.iter().enumerate() { match s.key { Some((c, v)) => { self.keyed.entry((s.table, c, v)).or_default().push(i); let cols = self.key_cols.entry(s.table).or_default(); if !cols.contains(&c) { cols.push(c); } } None => self.general.entry(s.table).or_default().push(i) } } }
}
pub struct Reactor { sql: Arc<Sql>, state: Arc<Mutex<State>>, pub debounce: Duration, pub reruns: AtomicU64, pub rerun_us: AtomicU64, pub events: AtomicU64, pub last_seq: AtomicU64, stop: Arc<AtomicBool> }

fn session_like(s: &Session) -> Session { Session { txn: None, user: s.user.clone(), roles: s.roles.clone(), attrs: s.attrs.clone(), last_seq: 0 } }
/// The predicates a single-table plan applies, including the ones hidden in its access path.
fn plan_preds(sql: &Sql, sess: &Session, sp: &SelectPlan, params: &[Value]) -> Result<Vec<CPred>, String> {
    let mut preds: Vec<Pred> = sp.preds.clone(); let sch = &sp.table.schema;
    match &sp.access {
        Access::Pk(e) => preds.push(Pred { col: sch.pk, cmp: Cmp::Eq, rhs: e.clone() }),
        Access::Unique(e) => { if let Some(u) = sch.unique { preds.push(Pred { col: u, cmp: Cmp::Eq, rhs: e.clone() }); } }
        Access::Range { lo, lo_excl, hi, hi_excl } => { preds.push(Pred { col: sch.pk, cmp: if *lo_excl { Cmp::Gt } else { Cmp::Ge }, rhs: lo.clone() }); preds.push(Pred { col: sch.pk, cmp: if *hi_excl { Cmp::Lt } else { Cmp::Le }, rhs: hi.clone() }); }
        Access::Index(ix, e) => preds.push(Pred { col: ix.col, cmp: Cmp::Eq, rhs: e.clone() }),
        Access::Scan => {}
    }
    preds.iter().map(|p| sql.compile(sess, &sp.table.schema, p, params)).collect()
}
fn tables_of(sp: &SelectPlan, out: &mut Vec<u16>) {
    out.push(sp.table.id);
    if let Some(j) = &sp.join { out.push(j.probe.id); for st in &j.steps { out.push(st.build.id); } }
    for (s, _) in &sp.subs { tables_of(s, out); }
    for p in &sp.preds { if let Expr::Set(s) | Expr::Scalar(s) = &p.rhs { tables_of(s, out); } }
    out.sort_unstable(); out.dedup();
}
fn key_of(row: &[Value], key_col: Option<usize>) -> Option<i64> { match (key_col, row) { (Some(c), r) => match r.get(c) { Some(Value::Int(i)) => Some(*i), _ => None }, _ => None } }

impl Reactor {
    pub fn start(sql: Arc<Sql>) -> Arc<Reactor> {
        sql.db.log.feeds.fetch_add(1, Ordering::Relaxed);  // keep the memory ring alive
        let r = Arc::new(Reactor { sql: sql.clone(), state: Arc::new(Mutex::new(State::default())), debounce: sql.cfg.live.debounce, reruns: AtomicU64::new(0), rerun_us: AtomicU64::new(0), events: AtomicU64::new(0), last_seq: AtomicU64::new(0), stop: Arc::new(AtomicBool::new(false)) });
        let r2 = r.clone(); std::thread::spawn(move || r2.run());
        r
    }
    fn run(&self) {
        let mut feed = self.sql.subscribe(self.sql.current_seq());
        loop {
            if self.stop.load(Ordering::Relaxed) { return; }
            // sleep until something is appended, or until a dirty query is due
            let due = { let st = self.state.lock().unwrap(); st.lives.iter().filter_map(|l| l.dirty_since).map(|d| (d + self.debounce).saturating_duration_since(Instant::now())).min() };
            self.sql.db.log.wait_feed(feed.last_seq(), due.unwrap_or(Duration::from_millis(20)).min(Duration::from_millis(20)));
            let recs = feed.poll_raw(); let now = Instant::now();
            if !recs.is_empty() {
                let mut st = self.state.lock().unwrap();
                for (seq, t, op, payload) in &recs {
                    self.last_seq.store(*seq, Ordering::Relaxed);
                    let is_row = payload.len() == t.schema.row_size;
                    // candidates: keyed subscriptions matching this row's values on each indexed column, plus the general ones
                    let mut cands: Vec<usize> = Vec::new();
                    if is_row { if let Some(cols) = st.key_cols.get(&t.id) { for &c in cols { if let Some(v) = read_key(&t.schema, payload, c) { if let Some(ix) = st.keyed.get(&(t.id, c, v)) { cands.extend_from_slice(ix); } } } } }
                    else if *op == OP_DELETE { for ((tid, _, _), ix) in st.keyed.iter() { if *tid == t.id { cands.extend_from_slice(ix); } } }  // a key-only delete cannot be filtered: deliver to all
                    if let Some(g) = st.general.get(&t.id) { cands.extend_from_slice(g); }
                    if !cands.is_empty() {
                        let pk = if is_row { t.pk_of(payload) } else { i64::from_le_bytes(payload[..8].try_into().unwrap()) };
                        let row = if is_row { Some((0..t.schema.columns.len()).map(|i| decode_col(&t.schema, payload, i)).collect()) } else { None };
                        let ch = Arc::new(Change { seq: *seq, table: t.schema.name.clone(), op: match *op { OP_INSERT => ChangeOp::Insert, OP_UPDATE => ChangeOp::Update, _ => ChangeOp::Delete }, pk, row });
                        for &i in &cands {
                            let sub = &st.subs[i];
                            let pass = if is_row { sub.preds.iter().all(|p| p.eval(payload)) && sub.policies.iter().all(|p| p.eval(payload)) } else { *op == OP_DELETE };
                            if !pass { continue; }
                            if !sub.q.push(Event::Change(ch.clone())) { let _ = sub.q.push(Event::Overflow { last_seq: *seq }); sub.q.closed.store(true, Ordering::Relaxed); }
                            self.events.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    for live in st.lives.iter_mut() {
                        if live.dirty_since.is_some() || !live.tables.contains(&t.id) { continue; }
                        let dirty = match &live.single {
                            Some((tid, preds, pols)) if *tid == t.id => { let pk = if is_row { t.pk_of(payload) } else { i64::from_le_bytes(payload[..8].try_into().unwrap()) }; (is_row && preds.iter().all(|p| p.eval(payload)) && pols.iter().all(|p| p.eval(payload))) || (live.key_col.is_some() && live.keys.contains(&pk)) || (!is_row && live.key_col.is_none()) }
                            _ => true,
                        };
                        if dirty { live.dirty_since = Some(now); }
                    }
                }
                let before = st.subs.len(); st.subs.retain(|s| !s.q.closed.load(Ordering::Relaxed)); if st.subs.len() != before { st.reindex(); } st.lives.retain(|l| !l.q.closed.load(Ordering::Relaxed));
            }
            // re-run the live queries whose debounce has passed
            let mut st = self.state.lock().unwrap();
            for live in st.lives.iter_mut() {
                let Some(d) = live.dirty_since else { continue }; if now.saturating_duration_since(d) < self.debounce { continue; }
                let t0 = Instant::now();
                let rows = match self.sql.execute_s(&mut live.sess, &live.prepared, &live.params) { Ok(r) => r, Err(_) => { live.dirty_since = None; continue; } };
                self.reruns.fetch_add(1, Ordering::Relaxed); self.rerun_us.fetch_add(t0.elapsed().as_micros() as u64, Ordering::Relaxed);
                live.dirty_since = None;
                if rows != live.result {
                    let (added, removed) = match live.key_col { Some(c) => { let old: HashSet<i64> = live.keys.clone(); let new: HashSet<i64> = rows.iter().filter_map(|r| key_of(r, Some(c))).collect(); (rows.iter().filter(|r| key_of(r, Some(c)).map(|k| !old.contains(&k)).unwrap_or(true)).cloned().collect(), live.result.iter().filter(|r| key_of(r, Some(c)).map(|k| !new.contains(&k)).unwrap_or(true)).cloned().collect()) } None => (rows.clone(), live.result.clone()) };
                    live.keys = rows.iter().filter_map(|r| key_of(r, live.key_col)).collect();
                    live.result = rows.clone();
                    if !live.q.push(Event::Live { seq: self.last_seq.load(Ordering::Relaxed), rows, added, removed }) { live.q.closed.store(true, Ordering::Relaxed); }
                    self.events.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
    /// Subscribe to the changes a single-table SELECT would see: its WHERE becomes the filter, the session's row policies apply.
    pub fn subscribe(&self, sess: &Session, sql: &str, params: &[Value]) -> Result<Subscription, String> {
        let p = self.sql.prepare(sql)?;
        let Plan::Select(sp) = &p.plan else { return Err("subscribe needs a SELECT".into()) };
        if sp.join.is_some() || !sp.subs.is_empty() { return Err("subscribe needs a single-table SELECT (watch handles joins)".into()); }
        let preds = plan_preds(&self.sql, sess, sp, params)?; let policies = self.sql.guard(sess, &sp.table, P_SELECT)?;
        // an equality on an INT column with a constant makes the subscription indexable
        let key = { let mut all: Vec<Pred> = sp.preds.clone(); match &sp.access { Access::Pk(e) => all.push(Pred { col: sp.table.schema.pk, cmp: Cmp::Eq, rhs: e.clone() }), Access::Index(ix, e) => all.push(Pred { col: ix.col, cmp: Cmp::Eq, rhs: e.clone() }), _ => {} }
            all.iter().find_map(|p| if p.cmp == Cmp::Eq && sp.table.schema.columns[p.col].ty == crate::table::ColType::Int { match &p.rhs { Expr::Lit(Value::Int(v)) => Some((p.col, *v)), Expr::Param(i) => match params.get(*i) { Some(Value::Int(v)) => Some((p.col, *v)), _ => None }, _ => None } } else { None }) };
        let q = Arc::new(Queue { items: Mutex::new(VecDeque::new()), cv: Condvar::new(), cap: self.sql.cfg.live.queue, closed: AtomicBool::new(false), delivered: AtomicU64::new(0) });
        { let mut st = self.state.lock().unwrap(); st.subs.push(Sub { table: sp.table.id, preds, policies, q: q.clone(), key }); st.reindex(); }
        Ok(Subscription { q })
    }
    /// Watch a query: returns its current result and a handle that receives `Event::Live` whenever the result changes.
    pub fn watch(&self, sess: &Session, sql: &str, params: &[Value]) -> Result<(Subscription, Vec<Vec<Value>>), String> {
        let p = self.sql.prepare(sql)?;
        let Plan::Select(sp) = &p.plan else { return Err("watch needs a SELECT".into()) };
        let mut tables = vec![]; tables_of(sp, &mut tables);
        let single = if sp.join.is_none() && sp.subs.is_empty() { Some((sp.table.id, plan_preds(&self.sql, sess, sp, params)?, self.sql.guard(sess, &sp.table, P_SELECT)?)) } else { None };
        let key_col = if sp.join.is_none() { sp.proj.iter().position(|pr| matches!(pr, Proj::Col(c) if *c == sp.table.schema.pk)) } else { None };
        let mut s = session_like(sess); let result = self.sql.execute_s(&mut s, &p, params)?;
        let q = Arc::new(Queue { items: Mutex::new(VecDeque::new()), cv: Condvar::new(), cap: 4096, closed: AtomicBool::new(false), delivered: AtomicU64::new(0) });
        let keys = result.iter().filter_map(|r| key_of(r, key_col)).collect();
        self.state.lock().unwrap().lives.push(Live { prepared: p.clone(), sess: s, params: params.to_vec(), tables, single, key_col, result: result.clone(), keys, dirty_since: None, q: q.clone() });
        Ok((Subscription { q }, result))
    }
    pub fn counts(&self) -> (usize, usize) { let st = self.state.lock().unwrap(); (st.subs.len(), st.lives.len()) }
}
impl Drop for Reactor { fn drop(&mut self) { self.stop.store(true, Ordering::Relaxed); self.sql.db.log.feeds.fetch_sub(1, Ordering::Relaxed); } }
