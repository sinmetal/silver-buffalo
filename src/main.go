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

	call := bq.Tabledata.List("sinpkmnms", "sample", "persons")
	call.MaxResults(10)
	list, err := call.Do()
	if err != nil {
		fmt.Errorf("%v", err)
	}

	buf, err := json.Marshal(list)
	if err != nil {
		fmt.Errorf("%v", err)
	}
	fmt.Println(string(buf))
}
