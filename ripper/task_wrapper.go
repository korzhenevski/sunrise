package ripper

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/outself/sunrise/manager"
	"github.com/outself/sunrise/rpc2"
	"math/rand"
	"sync"
	"time"
)

type Worker struct {
	Id       uint32
	ServerId uint32
	Client   *rpc2.Client
	tasks    map[uint32]*Ripper
	stop     chan bool
	stopped  bool
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func NewWorker(serverId uint32, serverAddr string) *Worker {
	return &Worker{
		Id:       rand.Uint32(),
		ServerId: serverId,
		Client:   rpc2.NewClient(serverAddr),
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

// корректная остановка с завершением записи
func (w *Worker) GracefulStop() {
	var wait sync.WaitGroup
	w.stopped = true
	for _, t := range w.tasks {
		wait.Add(1)
		go func(t *Ripper) {
			defer wait.Done()
			t.Stop()
		}(t)
	}
	wait.Wait()
}

func (w *Worker) RequestTask() <-chan *manager.Task {
	var err error
	w.Client.Dial()

	wait := time.Second
	c := make(chan *manager.Task)
	req := manager.ReserveRequest{WorkerId: w.Id, ServerId: w.ServerId}

	go func() {
		for {
			glog.V(2).Info("request task")
			task := new(manager.Task)
			err = w.Client.Call("Tracker.ReserveTask", req, task)
			if err != nil {
				glog.Warning(err)
				time.Sleep(wait)
				continue
			}

			if !task.Success {
				time.Sleep(wait)
				continue
			}

			// TODO: variable or channel?
			if w.stopped {
				glog.Info("stop requesting tasks")
				break
			}

			c <- task
		}
	}()

	return c
}

func (w *Worker) SpawnTask(task *manager.Task) {
	glog.Infof("Spawn new task: %+v", task)

	// prefix := fmt.Sprintf("tid=%d qid=%d sid=%d ", task.Id, task.QueueId, task.StreamId)
	// logger := log.New(w.ripperLog, prefix, log.LstdFlags)

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
			Error:  fmt.Sprint(err),
		}
		e := w.Client.Call("Tracker.RetryTask", req, res)
		if e != nil {
			glog.Warning("task retry call fail", e)
		}
	}
}
