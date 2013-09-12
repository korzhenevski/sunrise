package main

import (
	"flag"
	"github.com/golang/glog"
	"github.com/outself/sunrise/ripper"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	tracker := flag.String("tracker", "localhost:4242", "rpc-tracker address")
	serverId := flag.Int("sid", 0, "server id")
	flag.Parse()
	defer glog.Flush()

	if *serverId == 0 {
		glog.Fatal("specify server_id")
	}

	w := ripper.NewWorker(uint32(*serverId), *tracker)
	gracefulStop(w)
	w.Run()
}

func gracefulStop(w *ripper.Worker) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	signal.Notify(sig, syscall.SIGQUIT)
	go func() {
		<-sig
		w.GracefulStop()
		os.Exit(1)
	}()
}
