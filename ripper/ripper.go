package main

import (
	"flag"
	"log"
	"runtime"
)

func main() {
	tracker := flag.String("tracker", "localhost:4242", "rpc-tracker address")
	serverId := flag.Int("sid", 0, "server id")
	flag.Parse()

	if *serverId == 0 {
		log.Fatal("specify server_id")
	}

	log.Printf("Ripper running")
	log.Printf("* ServerId: %d", *serverId)
	log.Printf("* Runtime CPU: %d", runtime.NumCPU())
	log.Printf("* RPC-Tracker: %s", *tracker)
}
