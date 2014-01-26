package main

import (
	"github.com/yunge/sphinx"
	// "database/sql"
	// "fmt"
	// _ "github.com/go-sql-driver/mysql"
	"log"
	"net/http"
)

// Sphinx можно и нужно использовать. Поиск делать после драфта веба.

import _ "net/http/pprof"

type Radio struct {
	Id      int
	OwnerId int
	Title   string
}

func main() {
	go func() {
		log.Println(http.ListenAndServe(":6060", nil))
	}()

	sc := sphinx.NewClient().SetIndex("rt2").SetSqlServer("localhost", 9306).SetConnectTimeout(500)
	radio := &Radio{10, 100, "testing"}
	if err := sc.Insert(radio); err != nil {
		panic(err)
	}

	log.Print("ok")
}
