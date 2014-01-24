package main

import (
	"encoding/json"
	"log"
	"net/http"
)

func radioList(rw http.ResponseWriter, req *http.Request) {

}

func main() {
	http.HandleFunc("/radio.list", radioList)

	log.Fatal(http.ListenAndServe(":8081", nil))
}
