package main

import (
	"flag"
	"github.com/golang/glog"
	"github.com/outself/sunrise/backend"
	"labix.org/v2/mgo"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"time"
)

import _ "net/http/pprof"

func main() {
	dbUrl := flag.String("db", "localhost", "db url")
	dbName := flag.String("dbname", "test", "db name")
	listenAddr := flag.String("listen", ":4243", "rpc listen address")

	flag.Parse()
	defer glog.Flush()

	session, err := mgo.DialWithTimeout(*dbUrl, 1*time.Second)
	if err != nil {
		glog.Fatal("mongo dial error: ", err)
	}

	server := rpc.NewServer()
	server.RegisterName("Radio", backend.NewRadio(session.DB(*dbName)))

	l, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		glog.Fatal("listen error:", err)
	}

	glog.Infof("listen on %s...", *listenAddr)

	for {
		if conn, err := l.Accept(); err == nil {
			go server.ServeCodec(jsonrpc.NewServerCodec(conn))
		} else {
			glog.Error(err)
		}
	}
}
