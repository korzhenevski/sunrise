package main

import (
	"flag"
	"github.com/golang/glog"
	"github.com/outself/sunrise/manager"
	"labix.org/v2/mgo"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
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

	server := rpc.NewServer()
	manager := manager.New(session.DB(*dbName))
	server.RegisterName("Tracker", manager)

	l, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		glog.Fatal("listen error:", err)
	}

	glog.Info("tracker started...")

	go manager.LoopRespawnDeadChannels()

	for {
		if conn, err := l.Accept(); err == nil {
			go server.ServeCodec(jsonrpc.NewServerCodec(conn))
		} else {
			glog.Error(err)
		}
	}
}
