package main

import (
	"code.google.com/p/goauth2/oauth/jwt"
	bigquery "code.google.com/p/google-api-go-client/bigquery/v2"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"
)

func main() {
	iss := "777125258302-0n8r217cif8jcape5c1morlcb8j6i001@developer.gserviceaccount.com"
	scope := bigquery.BigqueryScope

	pem, err := ioutil.ReadFile("/Users/sinmetal/workspace/silver-buffalo/simpkmnms-9d4d8098a498.pem")
	token := jwt.NewToken(iss, scope, pem)

	transport, err := jwt.NewTransport(token)
	if err != nil {
		fmt.Errorf("%v", err)
	}
	client := transport.Client()
	bq, err := bigquery.New(client)
	if err != nil {
		fmt.Errorf("%v", err)
	}

	const url = "http://1.cp300demo1.appspot.com/"
	var i int = 0
	var wg sync.WaitGroup
	for {
		for j := 0; j < 50; j++ {
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
		_, err = bq.Tabledata.InsertAll("sinpkmnms", "dos", "progres20141122", &bigquery.TableDataInsertAllRequest{
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
