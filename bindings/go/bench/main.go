// Bench: the same workloads through our Go binding and through go-sqlite3 (cgo) via database/sql.
package main

import (
	"database/sql"
	"fmt"
	"os"
	"sync"
	"time"

	"dblab/bench/dblab"
	_ "github.com/mattn/go-sqlite3"
)

const N = 1_000_000

func main() {
	fmt.Println("=== Go bindings: ours (cgo over the C ABI) vs go-sqlite3 (cgo, database/sql); 1M rows ===")
	// ---- ours ----
	os.RemoveAll("../../data/nocow/gobench")
	db, err := dblab.Open("../../data/nocow/gobench", false)
	if err != nil {
		panic(err)
	}
	s := db.Session()
	if _, err := s.ExecText("CREATE TABLE t(id INTEGER PRIMARY KEY, score DOUBLE, country INT, name VARCHAR(32))"); err != nil {
		panic(err)
	}
	ins, _ := db.Prepare("INSERT INTO t VALUES (?,?,?,?)")
	t := time.Now()
	for id := int64(1); id <= N; id++ {
		if id%1000 == 1 {
			s.ExecText("BEGIN")
		}
		if _, err := s.Exec(ins, id, float64(id%1000), id%20, fmt.Sprintf("u%d", id)); err != nil {
			panic(err)
		}
		if id%1000 == 0 {
			s.ExecText("COMMIT")
		}
	}
	fmt.Printf("ours    load 1M rows, 1000-row txns          %.2f us/row\n", float64(time.Since(t).Microseconds())/N)
	get, _ := db.Prepare("SELECT score FROM t WHERE id = ?")
	t = time.Now()
	x := 0.0
	for i := 0; i < 200_000; i++ {
		r, _ := s.Exec(get, int64(i*7919%N+1))
		x += r[0][0].(float64)
	}
	fmt.Printf("ours    point read by id, 1 goroutine         %.2f us\n", float64(time.Since(t).Microseconds())/200_000)
	t = time.Now()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			ss := db.Session()
			defer ss.Close()
			for i := 0; i < 100_000; i++ {
				ss.Exec(get, int64((i*7919+g*1234567)%N+1))
			}
		}(g)
	}
	wg.Wait()
	fmt.Printf("ours    point reads, 8 goroutines             %.2fM/s\n", 0.8/time.Since(t).Seconds())
	t = time.Now()
	for id := int64(5_000_000); id < 5_050_000; id++ {
		s.Exec(ins, id, 1.0, id%20, fmt.Sprintf("u%d", id))
	}
	fmt.Printf("ours    single-row inserts, autocommit        %.2f us\n", float64(time.Since(t).Microseconds())/50_000)
	rng, _ := db.Prepare("SELECT id, score FROM t WHERE id >= ? AND id <= ?")
	t = time.Now()
	for i := 0; i < 1000; i++ {
		r, _ := s.Exec(rng, int64(i*1000+1), int64(i*1000+1000))
		if len(r) != 1000 {
			panic(len(r))
		}
	}
	fmt.Printf("ours    fetch 1000 rows by id range            %.1f us\n", float64(time.Since(t).Microseconds())/1000)
	ins.Close()
	get.Close()
	rng.Close()
	s.Close()
	db.Close()
	os.RemoveAll("../../data/nocow/gobench")
	_ = x
	// ---- go-sqlite3 via database/sql ----
	os.Remove("../../data/nocow/gobench.db")
	sq, err := sql.Open("sqlite3", "file:../../data/nocow/gobench.db?_journal_mode=WAL&_synchronous=NORMAL")
	if err != nil {
		panic(err)
	}
	sq.Exec("CREATE TABLE t(id INTEGER PRIMARY KEY, score REAL, country INTEGER, name TEXT)")
	t = time.Now()
	tx, _ := sq.Begin()
	sins, _ := tx.Prepare("INSERT INTO t VALUES (?,?,?,?)")
	for id := int64(1); id <= N; id++ {
		sins.Exec(id, float64(id%1000), id%20, fmt.Sprintf("u%d", id))
		if id%1000 == 0 {
			sins.Close()
			tx.Commit()
			if id < N {
				tx, _ = sq.Begin()
				sins, _ = tx.Prepare("INSERT INTO t VALUES (?,?,?,?)")
			}
		}
	}
	fmt.Printf("sqlite3 load 1M rows, 1000-row txns          %.2f us/row\n", float64(time.Since(t).Microseconds())/N)
	sget, _ := sq.Prepare("SELECT score FROM t WHERE id = ?")
	t = time.Now()
	for i := 0; i < 200_000; i++ {
		var sc float64
		sget.QueryRow(int64(i*7919%N + 1)).Scan(&sc)
		x += sc
	}
	fmt.Printf("sqlite3 point read by id, 1 goroutine         %.2f us\n", float64(time.Since(t).Microseconds())/200_000)
	t = time.Now()
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 100_000; i++ {
				var sc float64
				sget.QueryRow(int64((i*7919 + g*1234567) % N + 1)).Scan(&sc)
			}
		}(g)
	}
	wg.Wait()
	fmt.Printf("sqlite3 point reads, 8 goroutines             %.2fM/s\n", 0.8/time.Since(t).Seconds())
	sins2, _ := sq.Prepare("INSERT INTO t VALUES (?,?,?,?)")
	t = time.Now()
	for id := int64(5_000_000); id < 5_050_000; id++ {
		sins2.Exec(id, 1.0, id%20, fmt.Sprintf("u%d", id))
	}
	fmt.Printf("sqlite3 single-row inserts, autocommit        %.2f us\n", float64(time.Since(t).Microseconds())/50_000)
	srng, _ := sq.Prepare("SELECT id, score FROM t WHERE id >= ? AND id <= ?")
	t = time.Now()
	for i := 0; i < 1000; i++ {
		rows, _ := srng.Query(int64(i*1000+1), int64(i*1000+1000))
		n := 0
		for rows.Next() {
			var id int64
			var sc float64
			rows.Scan(&id, &sc)
			n++
		}
		rows.Close()
		if n != 1000 {
			panic(n)
		}
	}
	fmt.Printf("sqlite3 fetch 1000 rows by id range            %.1f us\n", float64(time.Since(t).Microseconds())/1000)
	sq.Close()
	os.Remove("../../data/nocow/gobench.db")
	os.Remove("../../data/nocow/gobench.db-wal")
	os.Remove("../../data/nocow/gobench.db-shm")
}
