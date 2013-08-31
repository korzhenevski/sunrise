package worker

import (
	"github.com/golang/glog"
	"io"
	"net/rpc"
	"sync"
	"time"
)

type RpcClient struct {
	mutex      sync.Mutex
	Client     *rpc.Client
	Addr       string
	MaxRetries int
}

func NewRpcClient(addr string) *RpcClient {
	c := &RpcClient{Addr: addr, MaxRetries: 30}
	return c
}

func (r *RpcClient) Dial() {
	var err error
	r.Client, err = rpc.DialHTTP("tcp", r.Addr)
	if err != nil {
		glog.Fatal(err)
	}
}

func (r *RpcClient) DialRetry() {
	var err error

	if r.Client != nil {
		r.Client.Close()
	}

	r.mutex.Lock()
	defer r.mutex.Unlock()

	retry := 0
	wait := 0
	for retry < r.MaxRetries {
		glog.Warningf("dial '%s'... retry %d after %dms wait", r.Addr, retry, wait)
		r.Client, err = rpc.DialHTTP("tcp", r.Addr)
		if err != nil {
			retry += 1
			wait = 500 * retry
			time.Sleep(time.Duration(wait) * time.Millisecond)
		} else {
			glog.Info("rpc reconnected")
			return
		}
	}

	if err != nil {
		panic(err)
	}
}

func (r *RpcClient) Call(serviceMethod string, args interface{}, reply interface{}) error {
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

	glog.V(2).Infof("rpc %s: %.3f ms", serviceMethod, (float64)(time.Now().Sub(startTime))/1e6)
	return err
}