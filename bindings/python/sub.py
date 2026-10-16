import os, shutil, sys, threading, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pydblab
d = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../data/nocow/pysub"); shutil.rmtree(d, ignore_errors=True)
db = pydblab.Database(d); s = db.session(); s.exec("CREATE TABLE orders(id INTEGER PRIMARY KEY, user_id INT, amount DOUBLE, ts INT)")
sub = db.session().subscribe("SELECT * FROM orders WHERE user_id = ?", (42,)); ins = db.prepare("INSERT INTO orders VALUES (?,?,?,?)")
lags = []
def consumer():
    while len(lags) < 1000:
        ev = sub.next(2000)
        if ev is None: break
        lags.append(time.perf_counter_ns() // 1000 - ev[6])
th = threading.Thread(target=consumer); th.start(); time.sleep(0.05)
for i in range(1000):
    for k in range(9): s.execute(ins, (1_000_000 + i * 10 + k, 1000 + k, 1.0, 0))
    s.execute(ins, (i + 1, 42, 9.5, time.perf_counter_ns() // 1000)); time.sleep(0.0003)
th.join(); lags.sort()
print(f"Python: {len(lags)} events for user 42 received (9,000 other rows filtered); insert -> event p50 {lags[len(lags)//2]} us, p99 {lags[len(lags)*99//100]} us")
del sub, ins, s, db; shutil.rmtree(d, ignore_errors=True)
