package main

import (
	"code.google.com/p/goauth2/oauth/jwt"
	bigquery "code.google.com/p/google-api-go-client/bigquery/v2"
	"encoding/json"
	"fmt"
	"io/ioutil"
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

	insert(bq)
	list(bq)
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

func insert(bq *bigquery.Service) {
	rows := make([]*bigquery.TableDataInsertAllRequestRows, 2)
	rows[0] = &bigquery.TableDataInsertAllRequestRows{
		Json: map[string]bigquery.JsonValue{
			"id":    "ida",
			"item1": "item1a",
			"item2": "item2a",
			"item3": "item3a",
			"date":  "2014/01/01",
		},
	}
	rows[1] = &bigquery.TableDataInsertAllRequestRows{
		Json: map[string]bigquery.JsonValue{
			"id":    "idb",
			"item1": "item1b",
			"item2": "item2b",
			"item3": "item3b",
			"date":  "2014/02/02",
		},
	}

	_, err := bq.Tabledata.InsertAll("sinpkmnms", "sample", "test", &bigquery.TableDataInsertAllRequest{
		Kind: "bigquery#tableDataInsertAllRequest",
		Rows: rows,
	}).Do()
	if err != nil {
		fmt.Errorf("%v", err)
		return
	}
}
