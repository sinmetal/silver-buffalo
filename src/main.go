package main

import (
	"fmt"
	"log"
	"net/http"
	"runtime"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"google.golang.org/api/bigquery/v2"
)

func main() {
	fmt.Println(runtime.NumCPU())
	fmt.Println(runtime.GOMAXPROCS(0))
	fmt.Println(runtime.NumGoroutine())
	runtime.GOMAXPROCS(runtime.NumCPU())

	client := &http.Client{
		Transport: &oauth2.Transport{
			Source: google.ComputeTokenSource(""),
		},
	}

	bq, err := bigquery.New(client)
	if err != nil {
		log.Fatalf("bigquery.New error, %v", err)
	}

	const url = "http://cp300demo1.appspot.com/"
	var i int = 0
	var wg sync.WaitGroup
	for {
		for j := 0; j < 10; j++ {
			wg.Add(1)
			go run(fmt.Sprint(i, ":", j), url, bq, &wg)
		}
		i++
		time.Sleep(1 * time.Second)
	}
	wg.Wait()
}

func run(id string, url string, bq *bigquery.Service, wg *sync.WaitGroup) {
	fmt.Println("start : ", id)

	startNano := time.Now().UnixNano()
	resp, err := http.Get(url)
	if err != nil {
		fmt.Println(err.Error())
		wg.Done()
		return
	}
	resp.Body.Close()
	endNano := time.Now().UnixNano()

	const ns = 1000000000
	const mics = 1000000

	start := time.Unix(startNano/ns, startNano%ns)
	end := time.Unix(endNano/ns, endNano%ns)

	err = insert(bq, url, resp.StatusCode, start.Unix(), end.Unix(), (endNano-startNano)/mics)
	if err != nil {
		fmt.Println("ng : ", id, err.Error())
	} else {
		fmt.Println("done : ", id)
	}
	wg.Done()
}

func insert(bq *bigquery.Service, url string, statusCode int, start int64, end int64, ms int64) error {
	rows := make([]*bigquery.TableDataInsertAllRequestRows, 1)
	rows[0] = &bigquery.TableDataInsertAllRequestRows{
		Json: map[string]bigquery.JsonValue{
			"url":         url,
			"status_code": statusCode,
			"start":       start,
			"end":         end,
			"progres_ms":  ms,
		},
	}

	var err error
	for i := 1; i < 10; i++ {
		_, err = bq.Tabledata.InsertAll("silver-buffalo-sinmetal", "silverbuffalo", "progres", &bigquery.TableDataInsertAllRequest{
			Kind: "bigquery#tableDataInsertAllRequest",
			Rows: rows,
		}).Do()
		if err != nil {
			fmt.Errorf("%v", err)
			time.Sleep(time.Duration(i) * time.Second)
		} else {
			break
		}
	}
	return err
}
