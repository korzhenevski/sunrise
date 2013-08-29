package worker

import (
	"github.com/outself/sunrise/manager"
	"log"
	"time"
)

type WorkerTask struct {
	task *manager.Task
	quit chan bool
}

func (w *WorkerTask) Stop() {
	w.quit <- true
}

func (w *WorkerTask) Run() {
	for {
		select {
		case <-w.quit:
			log.Printf("%d quit", w.task.Id)
			return
		default:
			// nothing
		}
		time.Sleep(1 * time.Second)
		log.Printf("%d working", w.task.Id)
	}
}

func NewWorkerTask(task *manager.Task) *WorkerTask {
	return &WorkerTask{task: task, quit: make(chan bool, 1)}
}
