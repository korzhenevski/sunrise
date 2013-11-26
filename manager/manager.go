package manager

import (
	"errors"
	// TODO: really use?
	// "github.com/cybersiddhu/golang-set"
	crypto_rand "crypto/rand"
	"github.com/golang/glog"
	"github.com/outself/sunrise/http2"
	"github.com/vova616/xxhash"
	"io"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"math/rand"
	"path"
	"strconv"
	"time"
)

// TODO
// add interface for add/remove Server, Volume
// http://zookeeper.apache.org/doc/trunk/zookeeperTutorial.html#sc_producerConsumerQueues

func init() {
	rand.Seed(time.Now().UnixNano())
}

const (
	TOUCH_TIMEOUT = 30
	DEFAULT_UA    = "WinampMPEG/5.0"
)

type Manager struct {
	q          *mgo.Collection
	air        *mgo.Collection
	ids        *mgo.Collection
	records    *mgo.Collection
	servers    *mgo.Collection
	volumes    *mgo.Collection
	tasklog    *mgo.Collection
	streaminfo *mgo.Collection
}

func New(db *mgo.Database) *Manager {
	manager := &Manager{}

	manager.q = db.C("queue")
	manager.q.EnsureIndexKey("task_id")

	manager.air = db.C("air")
	manager.air.EnsureIndexKey("pub_ts")

	manager.ids = db.C("ids")
	manager.records = db.C("records")
	manager.air.EnsureIndexKey("task_id")
	manager.air.EnsureIndexKey("track_id")

	manager.servers = db.C("servers")
	manager.volumes = db.C("volumes")
	manager.tasklog = db.C("tasklog")

	manager.streaminfo = db.C("streaminfo")
	manager.streaminfo.EnsureIndexKey("stream_id")

	return manager
}

type StreamInfo struct {
	Name     string `bson:"name"`
	StreamId uint32 `bson:"stream_id"`
}

type OpResult struct {
	Success bool
}

type Task struct {
	QueueId          uint32 `bson:"_id"`
	Id               uint32 `bson:"task_id"`
	StreamUrl        string `bson:"url"`
	StreamId         uint32 `bson:"stream_id"`
	ServerId         uint32 `bson:"server_id"`
	Record           bool   `bson:"record"`
	RecordDuration   uint32 `bson:"record_duration"`
	Time             uint32 `bson:"ts"`
	RetryInterval    uint32 `bson:"retry_ivl"`
	MinRetryInterval uint32 `bson:"min_retry_ivl"`
	MaxRetryInterval uint32 `bson:"max_retry_ivl"`
	UserAgent        string `bson:"user_agent"`
	OpResult         `bson:",omitempty"`
}

func (t *Task) NextRetryInterval() uint32 {
	if t.RetryInterval < t.MaxRetryInterval {
		if t.RetryInterval > 0 {
			t.RetryInterval = uint32(float32(t.RetryInterval) * 1.5 * (0.5 + rand.Float32()))
			if t.RetryInterval > t.MaxRetryInterval {
				t.RetryInterval = t.MaxRetryInterval
			}
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
	RecordId uint32
	// Хеш записанных данных
	DumpHash   uint32
	DumpSize   uint32
	VolumeId   uint32
	StreamMeta string
	// Продолжительность записи
	Duration uint32
	TrackId  uint32
	Dup      bool
}

type TrackResult struct {
	TrackId    uint32
	RecordId   uint32
	RecordPath string
	VolumeId   uint32
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
	RecordId uint32 `bson:"rid"`
	PubTime  uint32 `bson:"pub_ts"`
	// Айди предыдущего трека
	PrevId uint32 `bson:"pid"`
	// Трек успешно завершился
	EndTime uint32 `bson:"end_ts"`
	// следущий трек = db.air.findId({PrevId: current.Id})
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
	TaskId   uint32 `bson:"task_id"`
	// айдишник трека
	TrackId uint32 `bson:"track_id"`
	// Кешированный путь в разделе (vol)
	Path string `bson:"path"`
	// Хеш (xxhash)
	Hash uint32 `bson:"hash"`
	// Размер в Байтах
	Size     uint32 `bson:"size"`
	StreamId uint32 `bson:"stream_id"`
	Time     uint32 `bson:"ts"`
	EndTime  uint32 `bson:"end_ts"`
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
	return path.Join(baseDir, id[len(id)-1:], id+".mp3")
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
	QueueId uint32
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

	task.QueueId, err = m.nextId("task")
	if err != nil {
		return err
	}

	if err := m.q.Insert(task); err != nil {
		return err
	}

	res.QueueId = task.QueueId
	res.Success = true
	return nil
}

func (m *Manager) GetTask(queueId uint32, task *Task) error {
	err := m.q.FindId(queueId).One(task)
	if err == mgo.ErrNotFound {
		return nil
	} else if err != nil {
		return err
	}
	task.Success = true
	return nil
}

func (m *Manager) RemoveTask(queueId uint32, result *OpResult) error {
	err := m.q.RemoveId(queueId)
	if err == mgo.ErrNotFound {
		return nil
	} else if err != nil {
		return err
	}
	result.Success = true
	return nil
}

// TODO: может не создавать трек на lrd (limit record duration) записи ?
// или помечать как-то особенно? dup=true?
// rename track to air ?
func (m *Manager) NewTrack(req TrackRequest, result *TrackResult) error {
	ts := getTs()
	title := ExtractStreamTitle(req.StreamMeta)

	task := new(Task)
	err := m.q.Find(bson.M{"task_id": req.TaskId}).One(&task)
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

	if task.Record {
		vol, err := m.selectUploadVolume(task.ServerId)
		if err != nil {
			return err
		}

		record, err := m.newRecord(trackId, task, vol)
		if err != nil {
			return err
		}

		result.RecordId = record.Id
		result.RecordPath = record.Path
		result.LimitRecordDuration = task.RecordDuration
		result.VolumeId = vol.Id
	}

	result.TrackId = trackId

	// save air record
	track := &Track{
		Id:       trackId,
		Title:    title,
		Time:     ts,
		EndTime:  0,
		PubTime:  0,
		RecordId: result.RecordId,
		PrevId:   req.TrackId,
		StreamId: task.StreamId,
		TaskId:   task.Id,
	}
	if err := m.air.Insert(track); err != nil {
		return err
	}

	// завершаем трек
	if req.TrackId != 0 {
		glog.Info("end track")
		if err := m.endTrack(req); err != nil {
			return err
		}
	}

	result.Success = true
	return nil
}

func (m *Manager) EndTrack(req TrackRequest, reply *bool) error {
	return m.endTrack(req)
}

func (m *Manager) endTrack(req TrackRequest) error {
	var err error
	ts := getTs()
	// завершаем трек
	err = m.air.UpdateId(req.TrackId, bson.M{"$set": bson.M{"end_ts": ts, "duration": req.Duration}})
	if err != nil {
		return err
	}

	if req.RecordId == 0 {
		return nil
	}

	// завершаем запись
	err = m.records.UpdateId(req.RecordId, bson.M{"$set": bson.M{
		"size": req.DumpSize, "hash": req.DumpHash, "end_ts": ts}})
	if err != nil {
		return err
	}

	err = m.volumes.UpdateId(req.VolumeId, bson.M{"$inc": bson.M{"used": req.DumpSize}})
	if err != nil {
		return err
	}

	return nil
}

func (m *Manager) newRecord(trackId uint32, task *Task, vol *Volume) (*Record, error) {
	recordId, err := m.nextId("records")
	if err != nil {
		return nil, err
	}

	record := &Record{
		Id:       recordId,
		ServerId: task.ServerId,
		TaskId:   task.Id,
		VolumeId: vol.Id,
		Time:     uint32(time.Now().Unix()),
		Path:     GetRecordPath(recordId, vol.Path),
		TrackId:  trackId,
		StreamId: task.StreamId,
	}

	if err := m.records.Insert(record); err != nil {
		return nil, err
	}

	return record, nil
}

func (m *Manager) selectUploadVolume(serverId uint32) (*Volume, error) {
	return m.selectVolume(serverId, true)
}

func (m *Manager) selectVolume(serverId uint32, upload bool) (*Volume, error) {
	var result []Volume
	// выбираем все активные разделы
	where := bson.M{"online": true}
	if upload {
		// если раздел под загрузку, ищем на этом-же сервере
		where["server_id"] = serverId
		where["upload"] = true
	} else {
		where["server_id"] = bson.M{"$ne": serverId}
		//where["upload"] = false
	}
	err := m.volumes.Find(where).Iter().All(&result)
	if err == mgo.ErrNotFound {
		return nil, errors.New("no free volume")
	} else if err != nil {
		return nil, err
	}
	// random distribution
	return &result[rand.Intn(len(result))], nil
}

type ReserveRequest struct {
	ServerId uint32
	WorkerId uint32
}

func genTaskId(workerId uint32) uint32 {
	b := make([]byte, 10)
	_, err := io.ReadFull(crypto_rand.Reader, b)
	if err != nil {
		panic(err)
	}
	return xxhash.Checksum32Seed(b, workerId)
}

func (m *Manager) ReserveTask(req ReserveRequest, task *Task) error {
	ts := getTs()
	where := bson.M{"ts": bson.M{"$lt": ts}, "server_id": req.ServerId}
	change := mgo.Change{
		Update:    bson.M{"$set": bson.M{"ts": ts + TOUCH_TIMEOUT, "task_id": genTaskId(req.WorkerId)}},
		ReturnNew: true,
	}

	_, err := m.q.Find(where).Apply(change, task)
	if err == mgo.ErrNotFound {
		return nil
	} else if err != nil {
		return err
	}

	if len(task.UserAgent) == 0 {
		task.UserAgent = DEFAULT_UA
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
	where := bson.M{"task_id": bson.M{"$in": req.TaskId}, "server_id": req.ServerId}

	// делаем из taskId хеш
	obsoleteTaskId := make(map[uint32]bool)
	for _, tid := range req.TaskId {
		obsoleteTaskId[tid] = true
	}

	iter := m.q.Find(where).Select(bson.M{"task_id": 1}).Iter()
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
	update := bson.M{"$set": bson.M{
		"ts": getTs() + TOUCH_TIMEOUT,
		// "retry_ivl": 0,
	}}
	if _, err := m.q.UpdateAll(where, update); err != nil {
		return err
	}

	res.Success = true
	return nil
}

type RetryRequest struct {
	TaskId uint32
	Error  string
}

// Повтор задачи с экспоненциальной задержкой
// Task.MinRetryInterval: Минимальная задержка
// Task.RetryInterval: Текущая задержка, увеличивается случайно в диапазоне [0,75...2,25], по достижению MaxRetryInterval
func (m *Manager) RetryTask(req RetryRequest, res *OpResult) error {
	// логируем ошибку
	if err := m.logTaskResult(req.TaskId, bson.M{"type": "error", "error": req.Error}); err != nil {
		return err
	}

	task := new(Task)
	err := m.q.Find(bson.M{"task_id": req.TaskId}).One(task)
	if err == mgo.ErrNotFound {
		return nil
	} else if err != nil {
		return err
	}

	// увеличиваем интервал попыток
	ivl := task.NextRetryInterval()
	glog.Infof("retry task after %d", ivl)
	update := bson.M{"$set": bson.M{"ts": getTs() + ivl, "retry_ivl": ivl}}
	if err := m.q.Update(bson.M{"task_id": req.TaskId}, update); err != nil {
		return err
	}

	res.Success = true
	return nil
}

type HttpResponseLog struct {
	StreamId uint32
	TaskId   uint32
	Header   http2.Header
}

func (m *Manager) LogHttpResponse(log HttpResponseLog, reply *OpResult) error {
	// сбрасываем интервал попыток
	m.q.Update(bson.M{"task_id": log.TaskId}, bson.M{"$set": bson.M{"retry_ivl": 0}})

	// обновляем информацию о потоке из "icy" заголовков
	streamInfo := bson.M{
		"name":    log.Header.Get("icy-name"),
		"task_id": log.TaskId,
	}
	change := mgo.Change{
		Update:    bson.M{"$set": streamInfo},
		Upsert:    true,
		ReturnNew: true,
	}
	result := new(StreamInfo)
	if _, err := m.streaminfo.Find(bson.M{"_id": log.StreamId}).Apply(change, result); err != nil {
		return err
	}

	// логируем заголовки
	res := bson.M{"type": "http", "headers": log.Header}
	return m.logTaskResult(log.TaskId, res)
}

func (m *Manager) logTaskResult(taskId uint32, result bson.M) error {
	id, err := m.nextId("tasklog")
	if err != nil {
		return err
	}

	result["_id"] = id
	result["task_id"] = taskId
	result["ts"] = getTs()

	if err := m.tasklog.Insert(result); err != nil {
		return err
	}

	return nil
}

type RecordRequest struct {
	StreamId uint32
	From     time.Time
	To       time.Time
}

type RecordReply struct {
	Record []Record
}

func (m *Manager) GetRecord(req RecordRequest, reply *RecordReply) error {
	where := bson.M{
		"stream_id": req.StreamId,
		"ts":        bson.M{"$gte": uint32(req.From.Unix())},
		"end_ts":    bson.M{"$lte": uint32(req.To.Unix())},
	}
	err := m.records.Find(where).Sort("ts").Iter().All(&reply.Record)
	if err != nil {
		return err
	}
	return nil
}
