package main

import (
	"code.google.com/p/goauth2/oauth/jwt"
	bigquery "code.google.com/p/google-api-go-client/bigquery/v2"
	"encoding/json"
	"fmt"
	"io/ioutil"
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

	insert(bq, "http://localhost", time.Now().Unix(), time.Now().Unix())
	//list(bq)
}

func list(bq *bigquery.Service) {
	call := bq.Tabledata.List("sinpkmnms", "sample", "persons")
	call.MaxResults(10)
	list, err := call.Do()
	if err != nil {
		fmt.Errorf("%v", err)
		return
	}

	buf, err := json.Marshal(list)
	if err != nil {
		fmt.Errorf("%v", err)
		return
	}
	fmt.Println(string(buf))
}

func insert(bq *bigquery.Service, url string, start int64, end int64) {
	rows := make([]*bigquery.TableDataInsertAllRequestRows, 1)
	rows[0] = &bigquery.TableDataInsertAllRequestRows{
		Json: map[string]bigquery.JsonValue{
			"kind":        url,
			"start":       start,
			"end":         end,
			"progress_ms": end - start,
		},
	}

	_, err := bq.Tabledata.InsertAll("sinpkmnms", "dos", "progres", &bigquery.TableDataInsertAllRequest{
		Kind: "bigquery#tableDataInsertAllRequest",
		Rows: rows,
	}).Do()
	if err != nil {
		fmt.Errorf("%v", err)
		return
	}
}
