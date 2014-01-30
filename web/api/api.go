package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/golang/glog"
	"github.com/outself/sunrise/backend"
	"github.com/outself/sunrise/rpc2"
	"log"
	"net/http"
	// "net/rpc"
	// "net/rpc/jsonrpc"
)

type Response map[string]interface{}

func (r Response) String() (s string) {
	b, err := json.Marshal(r)
	if err != nil {
		s = ""
		return
	}
	s = string(b)
	return
}

type ApiContext struct {
	bd *rpc2.Client
}

var c ApiContext

func streamGet(rw http.ResponseWriter, req *http.Request) {
	var result backend.StreamResult
	if err := c.bd.Call("Stream.Get", backend.StreamGetParams{OwnerId: 1}, &result); err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		glog.Warning(err)
		return
	}

	rw.Header().Set("Content-Type", "application/json")
	fmt.Fprint(rw, Response{"success": true, "items": result.Items})
}

func main() {
	bdAddr := flag.String("bd", ":4243", "backend endpoint")
	listenAddr := flag.String("listen", ":8081", "HTTP listen host:port")
	flag.Parse()
	defer glog.Flush()

	c = ApiContext{rpc2.NewClient(*bdAddr, 30)}
	c.bd.Dial()

	glog.Infof("listen on %s", *listenAddr)

	http.HandleFunc("/stream.get", streamGet)
	log.Fatal(http.ListenAndServe(*listenAddr, nil))
}
