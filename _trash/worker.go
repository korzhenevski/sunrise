package worker

import (
	"github.com/outself/fastfm/tracker/server"
	"io"
	"log"
	"net/rpc"
	"sync"
	"time"
)

type Worker struct {
	ServerId uint32
	Client   *RpcClient
}

func New(serverId uint32, addr string) *Worker {
	return &Manager{ServerId: serverId, Client: NewRpcClient(addr)}
}

func (w *Worker) Dispatcher() {
	
}

func (w *Manager) Dispatcher() {
	var err error

	m.Client.Dial()

	for {
		task := new(server.Task)

		err = m.Client.Call("Tracker.ReserveTask", m.ServerId, task)
		if err != nil {
			panic(err)
		}

		if !task.Success {
			// log.Println("sleep...")
			time.Sleep(500 * time.Millisecond)
			continue
		}

		//log.Printf("new task: %+v", task)
		worker := NewWorker(task, m)
		go worker.Run()
	}
}
