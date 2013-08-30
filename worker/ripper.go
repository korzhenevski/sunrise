package worker

// spawn name worker<>ripper
// add task retry
// add task result (log http headers)
// add glog
// корректное завершение воркера
// air и record завершаются, новые записи не создаются
// при резервировании писать worker session - ws_id
// touch тоже делать с ws_id
// везде писать task_id ws_id
// возвращаем составной айдишник - <task_id>_<worker_id>_<wrk_req_id>
import (
	"errors"
	"github.com/outself/sunrise/manager"
	"github.com/outself/sunrise/radio"
	"github.com/vova616/xxhash"
	"hash"
	"log"
	"time"
)

type Ripper struct {
	task   *manager.Task
	dumper *radio.Dumper
	stream *radio.Stream
	worker *Worker
	meta   string
	metaTs int64
	track  *manager.TrackResult
	stop   chan bool
	hasher hash.Hash32
}

type TaskResult map[string]interface{}

func NewRipper(task *manager.Task, worker *Worker) *Ripper {
	return &Ripper{
		task:   task,
		worker: worker,
		dumper: &radio.Dumper{},
		track:  &manager.TrackResult{},
		stop:   make(chan bool, 1),
		hasher: xxhash.New(0),
		metaTs: time.Now().Unix(),
	}
}

var (
	ErrNoTask = errors.New("defunct task")
)

func (w *Ripper) Stop() {
	w.stop <- true
}

func (w *Ripper) Run() {
	log.Printf("connecting to %s...", w.task.StreamUrl)
	defer w.errorHandler()
	defer w.dumper.Close()

	var err error
	w.stream, err = radio.NewRadio(w.task.StreamUrl)
	if err != nil {
		panic(err)
	}
	defer w.stream.Close()

	log.Printf("Ripping '%s' (metaint %d, server '%s')", w.task.StreamUrl, w.stream.Metaint, w.stream.Header().Get("server"))

	w.logHttpResponse()

	metaChanged := true
	for {
		select {
		case <-w.stop:
			log.Printf("%d quit", w.task.Id)
			return
		default:
			// nothing
		}

		chunk, err := w.stream.ReadChunk()
		if err != nil {
			// TODO: really panic??
			panic(err)
		}

		w.hasher.Write(chunk.Data)

		// dumping data
		if len(w.track.RecordPath) > 0 {
			w.dumper.Write(chunk.Data)
		}

		// change meta, if present
		if len(chunk.Meta) > 0 && w.meta != chunk.Meta {
			w.meta = chunk.Meta
			metaChanged = true
		}

		// force rotate
		if w.track.LimitRecordDuration >= 0 && w.getDuration() >= w.track.LimitRecordDuration {
			metaChanged = true
		}

		// track meta
		if metaChanged {
			err := w.newTrack()
			if err == ErrNoTask {
				break
			} else if err != nil {
				panic(err)
			}

			// change dump path
			if len(w.track.RecordPath) > 0 {
				log.Printf("Record path: %s", w.track.RecordPath)
				w.dumper.Open(w.track.RecordPath)
			} else {
				w.dumper.Close()
			}

			metaChanged = false
		}
	}
}

func (r *Ripper) newTrack() error {
	dur := r.getDuration()
	data := &manager.TrackRequest{
		TaskId:     r.task.Id,
		RecordId:   r.track.RecordId,
		DumpHash:   r.hasher.Sum32(),
		DumpSize:   r.dumper.Written,
		StreamMeta: r.meta,
		Duration:   dur,
		TrackId:    r.track.TrackId,
	}
	r.hasher.Reset()
	r.metaTs = time.Now().Unix()

	r.track = new(manager.TrackResult)
	err := r.worker.Client.Call("Tracker.NewTrack", data, r.track)
	if err != nil {
		return err
	}

	if !r.track.Success {
		return ErrNoTask
	}

	log.Printf("new track %d (%d sec): %s", r.track.TrackId, dur, r.meta)
	return nil
}

func (r *Ripper) getDuration() uint32 {
	// TODO(outself): replace to proper time API
	return uint32(time.Now().Unix() - r.metaTs)
}

func (r *Ripper) errorHandler() {
	err := recover()
	if err != nil {
		log.Printf("error (task_id: %d '%s') - %s", r.task.Id, r.task.StreamUrl, err)
		r.worker.OnTaskExit(r.task.Id, err)
	}
	log.Println("worker exit")
}

func (r *Ripper) logHttpResponse() {
	l := manager.HttpResponseLog{
		TaskId: r.task.Id,
		Header: *r.stream.Header(),
	}
	var reply manager.OpResult
	err := r.worker.Client.Call("Tracker.LogHttpResponse", l, &reply)
	if err != nil {
		log.Println(err)
	}
}
