package worker

import (
	"github.com/outself/sunrise/manager"
	"log"
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
			log.Println("request task")
			task := new(manager.Task)
			err = w.Client.Call("Tracker.ReserveTask", w.ServerId, task)
			if err != nil {
				log.Println(err)
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
	log.Printf("Spawn new task: %+v", task)
	t := NewRipper(task, w)
	w.tasks[task.Id] = t
	go t.Run()
}

func (w *Worker) SendTaskTouch() {
	// log.Println("touch")
	// log.Printf("touch. %d running", len(w.tasks))

	if len(w.tasks) == 0 {
		return
	}

	req := manager.TouchRequest{ServerId: w.ServerId}
	for id, _ := range w.tasks {
		req.TaskId = append(req.TaskId, id)
	}

	res := new(manager.TouchResult)
	if err := w.Client.Call("Tracker.TouchTask", req, res); err != nil {
		log.Println(err)
		return
	}

	for _, tid := range res.ObsoleteTaskId {
		if task, ok := w.tasks[tid]; ok {
			delete(w.tasks, tid)
			log.Printf("stop task %d", tid)
			task.Stop()
		}
	}
}

func (w *Worker) OnTaskExit(taskId uint32, err error) {
	delete(w.tasks, taskId)
	if err != nil {
		res := new(manager.OpResult)
		e := w.Client.Call("Tracker.RetryTask", taskId, res)
		if e != nil {
			log.Println("task retry call fail", e)
		}
	}
}
