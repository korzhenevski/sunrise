package radio

import (
	"errors"
	"github.com/outself/sunrise/tracker/server"
	"github.com/vova616/xxhash"
	"hash"
	"log"
	"time"
)

type Ripper struct {
	task    *server.Task
	manager *Worker
	dumper  *Dumper
	stream  *Stream

	meta          string
	metaChangedAt int64
	track         *server.TrackResult
	stop          chan bool
	hasher        hash.Hash32
}

func NewRipper(task *server.Task, manager *Manager) *Ripper {
	return &Ripper{
		task:          task,
		manager:       manager,
		dumper:        &Dumper{},
		track:         &server.TrackResult{},
		stop:          make(chan bool),
		hasher:        xxhash.New(0),
		metaChangedAt: time.Now().Unix(),
	}
}

var (
	ErrNoTask = errors.New("defunct task")
)

func (w *Ripper) Run() {
	log.Printf("connecting to %s...", w.task.StreamUrl)
	defer w.checkRipperError()
	defer w.dumper.Close()

	var err error
	w.stream, err = NewRadio(w.task.StreamUrl)
	if err != nil {
		panic(err)
	}
	defer w.stream.Close()

	log.Printf("Ripping '%s' (metaint %d, server '%s')",
		w.task.StreamUrl, w.stream.metaint, w.stream.res.Header.Get("Server"))
	metaChanged := true

	for {
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
		if w.track.MaxDuration >= 0 && w.getDuration() >= w.track.MaxDuration {
			metaChanged = true
		}

		// track meta
		if metaChanged {
			err := w.trackMeta()
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

func (w *Ripper) trackMeta() error {
	data := &server.TrackMeta{
		TaskId:   w.task.Id,
		StreamId: w.task.StreamId,
		RecordId: w.track.RecordId,
		Checksum: w.hasher.Sum32(),
		Meta:     w.meta,
		Duration: w.getDuration(),
		PrevId:   w.track.MetaId,
	}

	err := w.manager.Client.Call("Tracker.TrackMeta", data, w.track)
	if err != nil {
		return err
	}

	if !w.track.Success {
		return ErrNoTask
	}

	w.hasher.Reset()
	log.Printf("meta changed: (%d) %s", w.track.MetaId, w.meta)
	w.metaChangedAt = time.Now().Unix()
	return nil
}

func (w *Ripper) getDuration() uint32 {
	// TODO(outself): replace to proper time API
	return uint32(time.Now().Unix() - w.metaChangedAt)
}

func (w *Ripper) checkRipperError() {
	if err := recover(); err != nil {
		log.Printf("worker error (task_id: %d '%s') - %s\n", w.task.Id, w.task.StreamUrl, err)
	}
	log.Println("worker exit")
}
