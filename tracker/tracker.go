package main

import (
	"flag"
	"github.com/outself/sunrise/manager"
	"labix.org/v2/mgo"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

import _ "net/http/pprof"

func main() {
	log.Println("Tracker RPC-Server")

	dbUrl := flag.String("db", "localhost", "db url")
	dbName := flag.String("dbname", "test", "db name")
	listenAddr := flag.String("listen", ":4242", "rpc listen address")
	flag.Parse()

	session, err := mgo.Dial(*dbUrl)
	if err != nil {
		log.Fatal("mongo dial error:", err)
	}

	rpc.Register(manager.New(session.DB(*dbName)))
	rpc.HandleHTTP()

	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatal("listen error:", err)
	}

	http.Serve(listener, nil)
}
