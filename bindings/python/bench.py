# Bench: the same workloads through our Python extension and through the standard sqlite3 module.
import os, shutil, sqlite3, sys, threading, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pydblab
N = 1_000_000
ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../data/nocow")
print("=== Python bindings: ours (PyO3 extension) vs the sqlite3 module (C); 1M rows ===")
d = os.path.join(ROOT, "pybench"); shutil.rmtree(d, ignore_errors=True)
db = pydblab.Database(d); s = db.session()
s.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, score DOUBLE, country INT, name VARCHAR(32))")
ins = db.prepare("INSERT INTO t VALUES (?,?,?,?)")
t = time.perf_counter()
for i in range(1, N + 1):
    if i % 1000 == 1: s.exec("BEGIN")
    s.execute(ins, (i, float(i % 1000), i % 20, f"u{i}"))
    if i % 1000 == 0: s.exec("COMMIT")
print(f"ours    load 1M rows, 1000-row txns          {(time.perf_counter()-t)*1e6/N:.2f} us/row")
get = db.prepare("SELECT score FROM t WHERE id = ?")
t = time.perf_counter(); x = 0.0
for i in range(200_000): x += s.execute(get, (i * 7919 % N + 1,))[0][0]
print(f"ours    point read by id, 1 thread            {(time.perf_counter()-t)*1e6/200_000:.2f} us")
def reads(g):
    ss = db.session()
    for i in range(100_000): ss.execute(get, ((i * 7919 + g * 1234567) % N + 1,))
t = time.perf_counter(); ths = [threading.Thread(target=reads, args=(g,)) for g in range(8)]; [th.start() for th in ths]; [th.join() for th in ths]
print(f"ours    point reads, 8 threads                {0.8/(time.perf_counter()-t):.2f}M/s")
t = time.perf_counter()
for i in range(5_000_000, 5_050_000): s.execute(ins, (i, 1.0, i % 20, f"u{i}"))
print(f"ours    single-row inserts, autocommit        {(time.perf_counter()-t)*1e6/50_000:.2f} us")
rng = db.prepare("SELECT id, score FROM t WHERE id >= ? AND id <= ?")
t = time.perf_counter()
for i in range(1000):
    r = s.execute(rng, (i * 1000 + 1, i * 1000 + 1000)); assert len(r) == 1000
print(f"ours    fetch 1000 rows by id range            {(time.perf_counter()-t)*1e6/1000:.1f} us")
del s, ins, get, rng, db; shutil.rmtree(d, ignore_errors=True)
# ---- sqlite3 ----
p = os.path.join(ROOT, "pybench.db")
for suf in ["", "-wal", "-shm"]:
    try: os.remove(p + suf)
    except FileNotFoundError: pass
c = sqlite3.connect(p, check_same_thread=False, isolation_level=None); c.execute("PRAGMA journal_mode=WAL"); c.execute("PRAGMA synchronous=NORMAL")
c.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, score REAL, country INTEGER, name TEXT)")
t = time.perf_counter()
for i in range(1, N + 1):
    if i % 1000 == 1: c.execute("BEGIN")
    c.execute("INSERT INTO t VALUES (?,?,?,?)", (i, float(i % 1000), i % 20, f"u{i}"))
    if i % 1000 == 0: c.execute("COMMIT")
print(f"sqlite3 load 1M rows, 1000-row txns          {(time.perf_counter()-t)*1e6/N:.2f} us/row")
t = time.perf_counter(); x = 0.0
for i in range(200_000): x += c.execute("SELECT score FROM t WHERE id = ?", (i * 7919 % N + 1,)).fetchone()[0]
print(f"sqlite3 point read by id, 1 thread            {(time.perf_counter()-t)*1e6/200_000:.2f} us")
def sreads(g):
    cc = sqlite3.connect(p, check_same_thread=False)
    for i in range(100_000): cc.execute("SELECT score FROM t WHERE id = ?", ((i * 7919 + g * 1234567) % N + 1,)).fetchone()
t = time.perf_counter(); ths = [threading.Thread(target=sreads, args=(g,)) for g in range(8)]; [th.start() for th in ths]; [th.join() for th in ths]
print(f"sqlite3 point reads, 8 threads                {0.8/(time.perf_counter()-t):.2f}M/s")
t = time.perf_counter()
for i in range(5_000_000, 5_050_000): c.execute("INSERT INTO t VALUES (?,?,?,?)", (i, 1.0, i % 20, f"u{i}"))
print(f"sqlite3 single-row inserts, autocommit        {(time.perf_counter()-t)*1e6/50_000:.2f} us")
t = time.perf_counter()
for i in range(1000):
    r = c.execute("SELECT id, score FROM t WHERE id >= ? AND id <= ?", (i * 1000 + 1, i * 1000 + 1000)).fetchall(); assert len(r) == 1000
print(f"sqlite3 fetch 1000 rows by id range            {(time.perf_counter()-t)*1e6/1000:.1f} us")
c.close()
for suf in ["", "-wal", "-shm"]:
    try: os.remove(p + suf)
    except FileNotFoundError: pass
