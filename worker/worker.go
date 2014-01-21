package main

import (
	"flag"
	"github.com/golang/glog"
	"github.com/outself/sunrise/ripper"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	log.SetFlags(0)

	tracker := flag.String("tracker", "localhost:4242", "rpc-tracker address")
	logname := flag.String("log", "", "ripper log")
	serverId := flag.Int("sid", 0, "server id")

	flag.Parse()
	defer glog.Flush()

	if *serverId == 0 {
		glog.Fatal("specify server_id")
	}

	if len(*logname) > 0 {
		logOutput, err := os.OpenFile(*logname, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			glog.Fatal(err)
		}
		log.SetOutput(logOutput)
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
