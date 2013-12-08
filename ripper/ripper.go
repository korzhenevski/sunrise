package ripper

import (
	"bytes"
	"errors"
	"github.com/golang/glog"
	"github.com/outself/sunrise/manager"
	"github.com/outself/sunrise/mp3"
	"github.com/outself/sunrise/radio"
	"github.com/vova616/xxhash"
	"hash"
	"time"
)

// TODO
// интегрировать mediaInfo в holdStream
// ~ chunk.Data - меньше копий в памяти
//
// Зависающие потоки: после нескольких дней работы, есть потоки которые висят,
// важно прибивать их в task_wrapper
//
// Сконвертировать файлы записей в блобы по 6 часов
//
// Выдавать в API каналы и записи к ним

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
	defer w.exitHandler()

	var err error
	w.stream, err = radio.NewRadio(w.task.StreamUrl, w.task.UserAgent)
	if err != nil {
		panic(err)
	}
	defer w.stream.Close()
	defer w.endTrack()

	// glog.Infof("Process '%s' metaint %d, server '%s'", w.task.StreamUrl, w.stream.Metaint, w.stream.Header().Get("server"))
	if !w.holdChannel() {
		return
	}

	var mediaInfo *mp3.FrameHeader
	metaChanged := true

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

		if mediaInfo == nil {
			mediaInfo, err = mp3.GetFirstFrame(bytes.NewReader(chunk.Data))
			if err != nil {
				panic(err)
			}
			glog.Infof("mediainfo: %+v", mediaInfo)
		}
		return

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
		if w.track.LimitRecordDuration > 0 && w.getDuration() >= w.track.LimitRecordDuration {
			glog.V(2).Infof("task %d record duration (%d) limit exceed", w.task.Id, w.track.LimitRecordDuration)
			metaChanged = true
		}

		// track meta
		if metaChanged {
			err := w.newTrack()
			if err == ErrNoTask {
				glog.Warningf("task %d new track return no task", w.task.Id)
				break
			} else if err != nil {
				panic(err)
			}

			// change dump path
			if len(w.track.RecordPath) > 0 {
				glog.Infof("task %d ripping to %s", w.task.Id, w.track.RecordPath)
				w.dumper.Open(w.track.RecordPath)
			} else {
				glog.V(2).Infof("task %d record closed", w.task.Id)
				w.dumper.Close()
			}

			metaChanged = false
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
	}
}

func (r *Ripper) newTrack() error {
	req := r.buildTrackRequest()

	r.hasher.Reset()
	r.metaTs = time.Now().Unix()

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

func (r *Ripper) holdChannel() bool {
	req := manager.ResponseInfo{
		StreamId: r.task.StreamId,
		TaskId:   r.task.Id,
		Header:   *r.stream.Header(),
	}

	reply := new(manager.OpResult)
	if err := r.worker.Client.Call("Tracker.HoldChannel", req, &reply); err != nil {
		glog.Warning(err)
		return false
	}

	return reply.Success
}
