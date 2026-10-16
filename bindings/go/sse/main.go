// SSE example (PocketBase shape): the app embeds the database, exposes GET /events?user=N as a Server-Sent Events
// stream backed by a filtered subscription, and here also plays the client to measure insert -> browser-side event.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/iluxav/sqweezdb/bindings/go/sqweezdb"
)

func main() {
	os.RemoveAll("../../data/nocow/sse")
	db, err := sqweezdb.Open("../../data/nocow/sse", false)
	if err != nil {
		panic(err)
	}
	s := db.Session()
	s.ExecText("CREATE TABLE orders(id INTEGER PRIMARY KEY, user_id INT, amount DOUBLE, ts INT)")
	mux := http.NewServeMux()
	mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		user, _ := strconv.Atoi(r.URL.Query().Get("user"))
		sess := db.Session()
		defer sess.Close()
		sub, err := sess.Subscribe("SELECT * FROM orders WHERE user_id = ?", int64(user))
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		defer sub.Close()
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		fl := w.(http.Flusher)
		fl.Flush()
		for {
			ev, err := sub.Next(200)
			if err != nil {
				fmt.Fprintf(w, "event: error\ndata: %s\n\n", err)
				fl.Flush()
				return
			}
			if ev == nil {
				if r.Context().Err() != nil {
					return
				}
				continue
			}
			b, _ := json.Marshal(map[string]any{"op": ev[0], "table": ev[1], "key": ev[2], "row": ev[3:]})
			fmt.Fprintf(w, "data: %s\n\n", b)
			fl.Flush()
		}
	})
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	go http.Serve(ln, mux)
	url := "http://" + ln.Addr().String() + "/events?user=42"
	fmt.Printf("SSE server on %s\n", url)
	// the client side: connect, then the app inserts 1000 orders for user 42 (and noise for others) and we time each event
	resp, err := http.Get(url)
	if err != nil {
		panic(err)
	}
	rd := bufio.NewReader(resp.Body)
	ins, _ := db.Prepare("INSERT INTO orders VALUES (?,?,?,?)")
	t0 := time.Now()
	now := func() int64 { return time.Since(t0).Microseconds() }
	done := make(chan []float64)
	go func() {
		lags := []float64{}
		for len(lags) < 1000 {
			line, err := rd.ReadString('\n')
			if err != nil {
				break
			}
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			var ev struct {
				Row []any `json:"row"`
			}
			json.Unmarshal([]byte(strings.TrimPrefix(strings.TrimSpace(line), "data: ")), &ev)
			if len(ev.Row) == 4 {
				ts := int64(ev.Row[3].(float64))
				lags = append(lags, float64(now()-ts))
			}
		}
		done <- lags
	}()
	time.Sleep(100 * time.Millisecond)
	for i := 0; i < 1000; i++ {
		for k := 0; k < 9; k++ { // noise: other users
			s.Exec(ins, int64(1_000_000+i*10+k), int64(1000+k), 1.0, int64(0))
		}
		s.Exec(ins, int64(i+1), int64(42), 9.5, now())
		time.Sleep(300 * time.Microsecond)
	}
	lags := <-done
	sort.Float64s(lags)
	fmt.Printf("Go SSE: %d events for user 42 received in the browser-side client out of 1000 (9,000 other users' rows filtered out); insert -> SSE event p50 %.0f us, p99 %.0f us\n", len(lags), lags[len(lags)/2], lags[len(lags)*99/100])
	resp.Body.Close()
	ins.Close()
	s.Close()
	db.Close()
	os.RemoveAll("../../data/nocow/sse")
}
