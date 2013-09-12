package worker

// spawn name worker<>ripper
import (
	"errors"
	"github.com/golang/glog"
	"github.com/outself/sunrise/manager"
	"github.com/outself/sunrise/radio"
	"github.com/vova616/xxhash"
	"hash"
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
	quit   chan bool
	hasher hash.Hash32
}

func NewRipper(task *manager.Task, worker *Worker) *Ripper {
	return &Ripper{
		task:   task,
		worker: worker,
		dumper: &radio.Dumper{},
		track:  &manager.TrackResult{},
		stop:   make(chan bool, 1),
		quit:   make(chan bool, 1),
		hasher: xxhash.New(0),
		metaTs: time.Now().Unix(),
	}
}

var (
	ErrNoTask = errors.New("defunct task")
)

func (r *Ripper) Stop() {
	r.stop <- true
	<-r.quit
}

func (w *Ripper) Run() {
	glog.V(2).Info("connect %s", w.task.StreamUrl)
	// закрытие дампера должно быть после exitHandler (defer is stack)
	// в exitHandler завершается трек и должен быть
	defer w.dumper.Close()
	defer w.endTrack()
	defer w.exitHandler()

	var err error
	w.stream, err = radio.NewRadio(w.task.StreamUrl, w.task.UserAgent)
	if err != nil {
		panic(err)
	}
	defer w.stream.Close()

	glog.Infof("Ripping '%s' metaint %d, server '%s'", w.task.StreamUrl, w.stream.Metaint, w.stream.Header().Get("server"))
	w.logHttpResponse()

	metaChanged := true
	dup := false
	for {
		select {
		case <-w.stop:
			glog.V(2).Infof("task %d quit", w.task.Id)
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
			glog.V(2).Infof("task %d stream meta changed", w.task.Id)
			w.meta = chunk.Meta
			metaChanged = true
		}

		// force rotate
		if w.track.LimitRecordDuration > 0 && w.getDuration() >= w.track.LimitRecordDuration {
			glog.V(2).Infof("task %d record duration (%d) limit exceed", w.task.Id, w.track.LimitRecordDuration)
			metaChanged = true
			dup = true
		}

		// track meta
		if metaChanged {
			err := w.newTrack(dup)
			if err == ErrNoTask {
				glog.Warningf("task %d new track return no task", w.task.Id)
				break
			} else if err != nil {
				panic(err)
			}

			// change dump path
			if len(w.track.RecordPath) > 0 {
				glog.Infof("task %d record path %s", w.task.Id, w.track.RecordPath)
				w.dumper.Open(w.track.RecordPath)
			} else {
				glog.V(2).Infof("task %d record closed", w.task.Id)
				w.dumper.Close()
			}

			metaChanged = false
			dup = false
		}
	}
}

func (r *Ripper) buildTrackRequest() *manager.TrackRequest {
	return &manager.TrackRequest{
		TaskId:     r.task.Id,
		RecordId:   r.track.RecordId,
		DumpHash:   r.hasher.Sum32(),
		DumpSize:   r.dumper.Written,
		StreamMeta: r.meta,
		Duration:   r.getDuration(),
		VolumeId:   r.track.VolumeId,
		TrackId:    r.track.TrackId,
		// Dup:        dup,
	}
}

// dup unused
func (r *Ripper) newTrack(dup bool) error {
	r.hasher.Reset()
	r.metaTs = time.Now().Unix()

	req := r.buildTrackRequest()
	r.track = new(manager.TrackResult)
	err := r.worker.Client.Call("Tracker.NewTrack", req, r.track)
	if err != nil {
		return err
	}

	if !r.track.Success {
		return ErrNoTask
	}

	glog.Infof("task %d new track %d '%s'", r.task.Id, r.track.TrackId, r.meta)
	return nil
}

func (r *Ripper) endTrack() {
	if r.track.TrackId == 0 {
		glog.V(2).Infof("task %d end track", r.task.Id)
		return
	}
	glog.V(2).Infof("task %d end track %d", r.task.Id, r.track.TrackId)
	var reply bool
	req := r.buildTrackRequest()
	err := r.worker.Client.Call("Tracker.EndTrack", req, &reply)
	if err != nil {
		glog.Warning("end track ", err)
	}
}

func (r *Ripper) getDuration() uint32 {
	// TODO(outself): replace to proper time API
	return uint32(time.Now().Unix() - r.metaTs)
}

func (r *Ripper) exitHandler() {
	err := recover()
	if err != nil {
		glog.Errorf("task %d url %s - %s", r.task.Id, r.task.StreamUrl, err)
	}
	r.worker.OnTaskExit(r.task.Id, err)
	r.quit <- true
	glog.Infof("task %d quitted", r.task.Id)
}

func (r *Ripper) logHttpResponse() {
	l := manager.HttpResponseLog{
		TaskId: r.task.Id,
		Header: *r.stream.Header(),
	}
	var reply manager.OpResult
	err := r.worker.Client.Call("Tracker.LogHttpResponse", l, &reply)
	if err != nil {
		glog.Error(err)
	}
}
