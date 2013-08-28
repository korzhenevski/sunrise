package manager

import (
	"errors"
	// TODO: really use?
	// "github.com/cybersiddhu/golang-set"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	// "log"
	"math/rand"
	"path"
	"strconv"
	"time"
)

// TODO
// Get air list (q: from, to)
// Serve air by HTTP (nginx)
// Task backoff retry, retry logging

// add to replicaCommit: record length, ID3, md5
// add interface for add/remove Server, Volume
// http://zookeeper.apache.org/doc/trunk/zookeeperTutorial.html#sc_producerConsumerQueues

// Normalize Meta: http://godoc.org/code.google.com/p/go.text/unicode/norm

// Listen records from any point
// Durable worker
// Extract queue specific logic to Worker class
// Test with 1024 streams

func init() {
	rand.Seed(time.Now().UnixNano())
}

type Manager struct {
	q       *mgo.Collection
	air     *mgo.Collection
	ids     *mgo.Collection
	records *mgo.Collection
	servers *mgo.Collection
	volumes *mgo.Collection
}

func New(db *mgo.Database) *Manager {
	manager := &Manager{}

	manager.q = db.C("queue")
	manager.air = db.C("air")
	manager.ids = db.C("ids")
	manager.records = db.C("records")
	manager.servers = db.C("servers")
	manager.volumes = db.C("volumes")

	return manager
}

type OpResult struct {
	Success bool
}

type Task struct {
	Id               uint32 `bson:"_id"`
	StreamUrl        string `bson:"url"`
	StreamId         uint32 `bson:"stream_id"`
	ServerId         uint32 `bson:"server_id"`
	Record           bool   `bson:"record"`
	RecordDuration   uint32 `bson:"record_duration"`
	Time             uint32 `bson:"ts"`
	RetryInterval    uint32 `bson:"retry_ivl"`
	MinRetryInterval uint32 `bson:"min_retry_ivl`
	MaxRetryInterval uint32 `bson:"max_retry_ivl"`
	OpResult
}

func (t *Task) NextRetryInterval() uint32 {
	if t.RetryInterval < t.MaxRetryInterval {
		if t.RetryInterval > 0 {
			t.RetryInterval = uint32(float32(t.RetryInterval) * 1.5 * (0.5 + rand.Float32()))
		} else {
			t.RetryInterval = t.MinRetryInterval
		}
	} else {
		t.RetryInterval = t.MaxRetryInterval
	}
	return t.RetryInterval
}

// Смена метаинформации в потоке
type TrackRequest struct {
	TaskId   uint32
	StreamId uint32
	RecordId uint32
	// Хеш записанных данных
	DumpHash   uint32
	StreamMeta string
	// Продолжительность записи
	Duration uint32
	TrackId  uint32
}

type TrackResult struct {
	TrackId    uint32
	RecordId   uint32
	RecordPath string
	// Максимальное время записи в дамп
	// после принудительно отправляется TrackRequest
	LimitRecordDuration uint32
	// Если задача не найдена, завершаем выполнение на воркере
	Success bool
}

type Track struct {
	Id       uint32 `bson:"_id"`
	StreamId uint32 `bson:"stream_id"`
	TaskId   uint32 `bson:"task_id"`
	Title    string `bson:"title"`
	Time     uint32 `bson:"ts"`
	EndTime  uint32 `bson:"end_ts"`
	RecordId uint32 `bson:"rid"`
	// Айди предыдущего трека
	PrevId uint32 `bson:"pid"`
	// Трек успешно завершился
	// следущий трек = db.air.findId({PrevId: current.Id})
	Ended bool `bson:"end"`
}

// Треки и записи разделены для возможности удаления
// старых аудиофайлов с сохранением истории треков

// Абстракция сохраненного эфира
// Описывает местоположение и свойства аудиофайла
type Record struct {
	Id uint32 `bson:"_id"`
	// Сервер записи
	ServerId uint32 `bson:"server_id"`
	// Раздел записи
	VolumeId uint32 `bson:"vol_id"`
	// Хеш (xxhash)
	Hash uint32 `bson:"hash"`
	// Размер в Байтах
	Size  uint32 `bson:"size"`
	Time  uint32 `bson:"ts"`
	Ended bool   `bson:"end"`
}

// Раздел записи
// По факту путь примонтированного диска
type Volume struct {
	Id       uint32 `bson:"_id"`
	ServerId uint32 `bson:"server_id"`
	// раздел доступен для записи
	IsOnline bool   `bson:"online"`
	IsUpload bool   `bson:"upload"`
	Path     string `bson:"path"`
}

type Server struct {
	Id  uint32 `bson:"_id"`
	Url string `bson:"url"`
}

func (s *Server) GetRecordUrl(record *Record) string {
	return s.Url + GetRecordPath(record.Id, strconv.Itoa(int(record.VolumeId)))
}

type NextId struct {
	Next uint32
}

func GetRecordPath(recordId uint32, baseDir string) string {
	id := strconv.Itoa(int(recordId))
	return path.Join(baseDir, id[0:1], id+".mp3")
}

func getTs() uint32 {
	return uint32(time.Now().Unix())
}

// Generate increment objectId stored in "ids" collection
// { _id: 'object_kind', next: '<next_id>'}
func (m *Manager) nextId(kind string) (uint32, error) {
	change := mgo.Change{
		Update:    bson.M{"$inc": bson.M{"next": 1}},
		Upsert:    true,
		ReturnNew: true,
	}

	result := new(NextId)
	if _, err := m.ids.Find(bson.M{"_id": kind}).Apply(change, result); err != nil {
		return 0, err
	}

	return result.Next, nil
}

type PutResult struct {
	TaskId  uint32
	Success bool
}

func (m *Manager) PutTask(task Task, res *PutResult) error {
	var err error
	if len(task.StreamUrl) == 0 {
		return errors.New("StreamUrl required")
	}

	if task.ServerId == 0 {
		return errors.New("ServerId required")
	}

	if task.MinRetryInterval == 0 {
		return errors.New("MinRetryInterval required")
	}

	if task.StreamUrl, err = NormalizeUrl(task.StreamUrl); err != nil {
		return err
	}

	task.Id, err = m.nextId("task")
	if err != nil {
		return err
	}

	if err := m.q.Insert(task); err != nil {
		return err
	}

	res.TaskId = task.Id
	res.Success = true
	return nil
}

func (m *Manager) GetTask(taskId uint32, task *Task) error {
	err := m.q.FindId(taskId).One(task)
	if err == mgo.ErrNotFound {
		return nil
	} else if err != nil {
		return err
	}
	task.Success = true
	return nil
}

func (m *Manager) RemoveTask(taskId uint32, result *OpResult) error {
	err := m.q.RemoveId(taskId)
	if err == mgo.ErrNotFound {
		return nil
	} else if err != nil {
		return err
	}
	result.Success = true
	return nil
}

func (m *Manager) NewTrack(req TrackRequest, result *TrackResult) error {
	ts := getTs()
	title := ExtractStreamTitle(req.StreamMeta)

	task := new(Task)
	err := m.q.FindId(req.TaskId).One(&task)
	if err == mgo.ErrNotFound {
		result.Success = false
		return nil
	} else if err != nil {
		return err
	}

	trackId, err := m.nextId("air")
	if err != nil {
		return err
	}

	vol, err := m.selectUploadVolume(task.ServerId)
	if err != nil {
		return err
	}

	record, err := m.newRecord(task.ServerId, vol.Id)
	if err != nil {
		return err
	}

	result.RecordId = record.Id
	result.RecordPath = GetRecordPath(record.Id, vol.Path)
	result.TrackId = trackId
	result.LimitRecordDuration = task.RecordDuration

	// save air record
	track := &Track{
		Id:       trackId,
		StreamId: req.StreamId,
		Title:    title,
		Time:     ts,
		EndTime:  0,
		RecordId: result.RecordId,
		PrevId:   req.TrackId,
		Ended:    false,
	}
	if err := m.air.Insert(track); err != nil {
		return err
	}

	// end previous
	if req.TrackId != 0 {
		// end air
		change := bson.M{"$set": bson.M{"end": true, "end_ts": ts, "duration": req.Duration}}
		if err := m.air.UpdateId(req.TrackId, change); err != nil {
			return err
		}

		if req.RecordId > 0 {
			m.records.UpdateId(req.RecordId, bson.M{"$set": bson.M{"end": true, "hash": req.DumpHash}})
			if err != nil {
				return err
			}
		}
	}

	result.Success = true
	return nil
}

func (m *Manager) selectUploadVolume(serverId uint32) (*Volume, error) {
	return m.selectVolume(serverId, true)
}

func (m *Manager) selectVolume(serverId uint32, upload bool) (*Volume, error) {
	var result []Volume
	// select any online volume not in source server
	where := bson.M{"server_id": bson.M{"$ne": serverId}, "online": true, "upload": upload}
	err := m.volumes.Find(where).Iter().All(&result)
	if err == mgo.ErrNotFound {
		return nil, errors.New("no free volume")
	} else if err != nil {
		return nil, err
	}
	// random distribution
	return &result[rand.Intn(len(result))], nil
}

func (m *Manager) newRecord(serverId uint32, volumeId uint32) (*Record, error) {
	record := &Record{
		ServerId: serverId,
		VolumeId: volumeId,
		Time:     uint32(time.Now().Unix()),
		Ended:    false,
	}

	var err error
	if record.Id, err = m.nextId("records"); err != nil {
		return nil, err
	}

	if err := m.records.Insert(record); err != nil {
		return nil, err
	}

	return record, nil
}

// add backoff retry
func (m *Manager) ReserveTask(serverId uint32, task *Task) error {
	ts := getTs()
	where := bson.M{"ts": bson.M{"$lt": ts - 20}}
	change := mgo.Change{
		Update:    bson.M{"$set": bson.M{"ts": ts, "server_id": serverId}},
		ReturnNew: true,
	}

	_, err := m.q.Find(where).Apply(change, task)
	if err == mgo.ErrNotFound {
		return nil
	} else if err != nil {
		return err
	}

	task.Success = true
	return nil
}

type TouchRequest struct {
	ServerId uint32
	TaskId   []uint32
}

type TouchResult struct {
	ObsoleteTaskId []uint32
	Success        bool
}

// Обновляем время последней активности по списку задач от воркера
// Возвращает список неактивных задач
func (m *Manager) TouchTask(req TouchRequest, res *TouchResult) error {
	where := bson.M{"_id": bson.M{"$in": req.TaskId}, "server_id": req.ServerId}

	// делаем из taskId хеш
	obsoleteTaskId := make(map[uint32]bool)
	for _, tid := range req.TaskId {
		obsoleteTaskId[tid] = true
	}

	iter := m.q.Find(where).Select(bson.M{"_id": 1}).Iter()
	var t Task
	for iter.Next(&t) {
		// удаляем все найденные таски
		delete(obsoleteTaskId, t.Id)
	}
	if err := iter.Close(); err != nil {
		return err
	}

	// копируем в результат несуществующие задачи
	res.ObsoleteTaskId = make([]uint32, 0, len(obsoleteTaskId))
	for tid, _ := range obsoleteTaskId {
		res.ObsoleteTaskId = append(res.ObsoleteTaskId, tid)
	}

	// обновляем время для всех задач
	if _, err := m.q.UpdateAll(where, bson.M{"$set": bson.M{"ts": getTs()}}); err != nil {
		return err
	}

	res.Success = true
	return nil
}

// Повтор задачи с экспоненциальной задержкой
// Task.MinRetryInterval: Минимальная задержка
// Task.RetryInterval: Текущая задержка, увеличивается случайно в диапазоне [0,75...2,25], по достижению MaxRetryInterval
func (m *Manager) RetryTask(taskId uint32, res *OpResult) error {
	task := new(Task)
	err := m.q.FindId(taskId).One(task)
	if err == mgo.ErrNotFound {
		return nil
	} else if err != nil {
		return err
	}

	task.NextRetryInterval()
	update := bson.M{"ts": getTs() + task.RetryInterval, "retry_ivl": task.RetryInterval}
	if err := m.q.UpdateId(taskId, update); err != nil {
		return err
	}

	res.Success = true
	return nil
}
