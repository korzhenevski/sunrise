package rpc2

import (
	"github.com/golang/glog"
	"io"
	"net/rpc"
	"sync"
	"time"
)

type Client struct {
	mutex      sync.Mutex
	Client     *rpc.Client
	Addr       string
	MaxRetries int
}

func NewClient(addr string) *Client {
	c := &Client{Addr: addr, MaxRetries: 30}
	return c
}

func (r *Client) Dial() {
	var err error
	r.Client, err = rpc.DialHTTP("tcp", r.Addr)
	if err != nil {
		glog.Fatal(err)
	}
}

func (r *Client) DialRetry() {
	var err error

	if r.Client != nil {
		r.Client.Close()
	}

	r.mutex.Lock()
	defer r.mutex.Unlock()

	retry := 0
	wait := 0
	for retry < r.MaxRetries {
		glog.Warningf("retry connect after %d ms", wait)
		r.Client, err = rpc.DialHTTP("tcp", r.Addr)
		if err != nil {
			retry += 1
			wait = 500 * retry
			time.Sleep(time.Duration(wait) * time.Millisecond)
		} else {
			glog.Info("reconnected")
			return
		}
	}

	if err != nil {
		panic(err)
	}
}

func (r *Client) Call(serviceMethod string, args interface{}, reply interface{}) error {
	startTime := time.Now()
	var err error
	for {
		r.mutex.Lock()
		err = r.Client.Call(serviceMethod, args, reply)
		r.mutex.Unlock()
		if err == rpc.ErrShutdown || err == io.ErrUnexpectedEOF || err == io.EOF {
			r.DialRetry()
			continue
		}
		break
	}

	glog.V(2).Infof("%s: %.3f ms", serviceMethod, (float64)(time.Now().Sub(startTime))/1e6)
	return err
}
