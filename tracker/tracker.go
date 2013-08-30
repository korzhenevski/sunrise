package main

import (
	"flag"
	"github.com/golang/glog"
	"github.com/outself/sunrise/manager"
	"labix.org/v2/mgo"
	"net"
	"net/http"
	"net/rpc"
)

import _ "net/http/pprof"

func main() {
	dbUrl := flag.String("db", "localhost", "db url")
	dbName := flag.String("dbname", "test", "db name")
	listenAddr := flag.String("listen", ":4242", "rpc listen address")
	flag.Parse()
	defer glog.Flush()

	session, err := mgo.Dial(*dbUrl)
	if err != nil {
		glog.Fatal("mongo dial error:", err)
	}

	rpc.RegisterName("Tracker", manager.New(session.DB(*dbName)))
	rpc.HandleHTTP()

	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		glog.Fatal("listen error:", err)
	}

	http.Serve(listener, nil)
}
