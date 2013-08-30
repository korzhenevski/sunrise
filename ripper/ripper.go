package main

import (
	"flag"
	"github.com/golang/glog"
	"github.com/outself/sunrise/worker"
)

func main() {
	tracker := flag.String("tracker", "localhost:4242", "rpc-tracker address")
	serverId := flag.Int("sid", 0, "server id")
	flag.Parse()
	defer glog.Flush()

	if *serverId == 0 {
		glog.Fatal("specify server_id")
	}

	w := worker.NewWorker(uint32(*serverId), *tracker)
	w.Run()
}
