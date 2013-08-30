package worker

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/outself/sunrise/manager"
	"time"
)

type Worker struct {
	Client   *RpcClient
	ServerId uint32
	tasks    map[uint32]*Ripper
	stop     chan bool
}

func NewWorker(serverId uint32, serverAddr string) *Worker {
	return &Worker{
		ServerId: serverId,
		Client:   NewRpcClient(serverAddr),
		tasks:    make(map[uint32]*Ripper),
		stop:     make(chan bool),
	}
}

func (w *Worker) Run() {
	touchIvl := time.Tick(5 * time.Second)
	taskq := w.RequestTask()
	for {
		select {
		case <-touchIvl:
			w.SendTaskTouch()
		case task := <-taskq:
			w.SpawnTask(task)
		case <-w.stop:
			return
		}
	}
}

func (w *Worker) RequestTask() <-chan *manager.Task {
	var err error
	w.Client.Dial()

	wait := time.Second
	c := make(chan *manager.Task)

	go func() {
		for {
			glog.V(2).Info("request task")
			task := new(manager.Task)
			err = w.Client.Call("Tracker.ReserveTask", w.ServerId, task)
			if err != nil {
				glog.Warning(err)
				time.Sleep(wait)
				continue
			}

			if !task.Success {
				time.Sleep(wait)
				continue
			}

			c <- task
		}
	}()

	return c
}

func (w *Worker) SpawnTask(task *manager.Task) {
	glog.Infof("Spawn new task: %+v", task)
	t := NewRipper(task, w)
	w.tasks[task.Id] = t
	go t.Run()
}

func (w *Worker) SendTaskTouch() {
	glog.V(2).Infof("send %d task touch request", len(w.tasks))
	if len(w.tasks) == 0 {
		return
	}

	req := manager.TouchRequest{ServerId: w.ServerId}
	for id, _ := range w.tasks {
		req.TaskId = append(req.TaskId, id)
	}

	res := new(manager.TouchResult)
	if err := w.Client.Call("Tracker.TouchTask", req, res); err != nil {
		glog.Warning(err)
		return
	}

	for _, tid := range res.ObsoleteTaskId {
		if task, ok := w.tasks[tid]; ok {
			delete(w.tasks, tid)
			glog.V(2).Infof("stop task %d", tid)
			task.Stop()
		}
	}
}

func (w *Worker) OnTaskExit(taskId uint32, err interface{}) {
	delete(w.tasks, taskId)
	if err != nil {
		res := new(manager.OpResult)
		req := manager.RetryRequest{
			TaskId: taskId,
			Error:  fmt.Sprintf("%s", err),
		}
		e := w.Client.Call("Tracker.RetryTask", req, res)
		if e != nil {
			glog.Warning("task retry call fail", e)
		}
	}
}
