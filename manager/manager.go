package manager

import (
	"errors"
	// TODO: really use?
	// "github.com/cybersiddhu/golang-set"
	"fmt"
	"github.com/golang/glog"
	"github.com/kr/pretty"
	"github.com/outself/sunrise/http2"
	// "github.com/outself/sunrise/mp3"
	"hash/crc32"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"math/rand"
	"path"
	"strconv"
	"sync"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

const (
	TOUCH_TIMEOUT = 15
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
	ch         *mgo.Collection
	holder     sync.Mutex
	respawner  sync.Mutex
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

	manager.ch = db.C("ch")
	manager.ch.EnsureIndexKey("ts")

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
	ChannelId        uint32 `bson:"channel_id"`
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
	Hash uint32
	// размер текущего трека
	Size int64
	// размер всей записи
	RecordSize int64
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
	Id        uint32 `bson:"_id"`
	StreamId  uint32 `bson:"stream_id"`
	TaskId    uint32 `bson:"task_id"`
	ChannelId uint32 `bson:"channel_id"`
	Title     string `bson:"title"`
	Time      uint32 `bson:"ts"`
	RecordId  uint32 `bson:"rid"`
	PubTime   uint32 `bson:"pub_ts"`
	// Айди предыдущего трека
	PrevId uint32 `bson:"pid"`
	// Трек успешно завершился
	EndTime uint32 `bson:"end_ts"`
	Offset  int64  `bson:"offset"`
	Size    int64  `bson:"size"`
	// следущий трек = db.air.findId({PrevId: current.Id})
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
	// Кешированный путь в разделе (vol)
	Path string `bson:"path"`
	// Размер в Байтах
	Size     uint32 `bson:"size"`
	StreamId uint32 `bson:"stream_id"`
	Time     uint32 `bson:"ts"`
	EndTime  uint32 `bson:"end_ts"`
}

func GetRecordPath(recordId uint32, baseDir string) string {
	id := strconv.Itoa(int(recordId))
	return path.Join(baseDir, id[len(id)-1:], id+".mp3")
}

func (m *Manager) newRecord(task *Task, vol *Volume) (*Record, error) {
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
		StreamId: task.StreamId,
	}

	if err := m.records.Insert(record); err != nil {
		return nil, err
	}

	return record, nil
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
		// TODO: rename to RecordLimit
		// TODO: что нибудь сделать с повторными треками
		result.LimitRecordDuration = task.RecordDuration

		record := new(Record)
		if req.RecordId == 0 || req.Duration >= result.LimitRecordDuration {
			vol, err := m.selectUploadVolume(task.ServerId)
			if err != nil {
				return err
			}

			var e error
			record, e = m.newRecord(task, vol)
			if e != nil {
				return err
			}
		} else {
			change := mgo.Change{
				Update:    bson.M{"$set": bson.M{"size": req.RecordSize}},
				ReturnNew: true,
			}

			_, err := m.records.Find(bson.M{"_id": req.RecordId}).Apply(change, &record)
			if err == mgo.ErrNotFound {
				result.Success = false
				return nil
			} else if err != nil {
				return err
			}
		}

		pretty.Println("Record:", record)

		result.RecordId = record.Id
		result.RecordPath = record.Path
		result.VolumeId = record.VolumeId
	}

	result.TrackId = trackId

	// save air record
	track := &Track{
		Id:        trackId,
		Title:     title,
		Time:      ts,
		EndTime:   0,
		PubTime:   0,
		ChannelId: task.ChannelId,
		RecordId:  result.RecordId,
		PrevId:    req.TrackId,
		Size:      req.Size,
		Offset:    req.RecordSize,
		StreamId:  task.StreamId,
		TaskId:    task.Id,
	}

	if err := m.air.Insert(track); err != nil {
		return err
	}

	pretty.Println("track:", track)
	// glog.Infof("track: %+v", track)

	// обновляем статистику по текущему треку
	m.q.Update(bson.M{"task_id": req.TaskId}, bson.M{"$set": bson.M{
		"runtime.track_id": trackId,
		"runtime.title":    title,
		"runtime.ts":       getTs(),
	}})

	// завершаем трек
	if req.TrackId != 0 {
		// glog.Info("end track")
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
	err = m.records.UpdateId(req.RecordId, bson.M{"$set": bson.M{"size": req.RecordSize, "end_ts": getTs()}})
	if err != nil {
		return err
	}

	err = m.volumes.UpdateId(req.VolumeId, bson.M{"$inc": bson.M{"used": req.RecordSize}})
	if err != nil {
		return err
	}

	return nil
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

func (m *Manager) ReserveTask(req ReserveRequest, task *Task) error {
	ts := getTs()
	where := bson.M{"ts": bson.M{"$lt": ts}, "server_id": req.ServerId}
	change := mgo.Change{
		Update: bson.M{
			"$unset": bson.M{"runtime": 1, "error": 1},
			"$set":   bson.M{"ts": ts + TOUCH_TIMEOUT, "task_id": genTaskId(req.WorkerId)},
		},
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

	// update channel ts
	_, err := m.ch.UpdateAll(bson.M{"task_id": bson.M{"$in": req.TaskId}}, bson.M{"$set": bson.M{"ts": getTs() + TOUCH_TIMEOUT}})
	if err != nil {
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
	glog.Warningf("retry task %d after %d", req.TaskId, ivl)
	update := bson.M{
		"$unset": bson.M{"task_id": 1, "runtime": 1},
		"$set":   bson.M{"ts": getTs() + ivl, "error": req.Error, "retry_ivl": ivl},
	}
	if err := m.q.Update(bson.M{"task_id": req.TaskId}, update); err != nil {
		return err
	}

	res.Success = true
	return nil
}

type ResponseInfo struct {
	StreamId uint32
	TaskId   uint32
	Header   http2.Header
	// MediaInfo mp3.FrameHeader
}

type Channel struct {
	Id       uint32 `bson:"_id"`
	Name     string `bson:"name"`
	TaskId   uint32 `bson:"task_id"`
	StreamId uint32 `bson:"stream_id"`
	Time     uint32 `bson:"ts"`
}

// у потока может быть куча зеркал,
// после соединения, воркер присылает заголовки
// извлекаем оттуда название потока (icy-name)
// и записываем лок в базу по идентификатору задачи
func (m *Manager) HoldChannel(info ResponseInfo, result *OpResult) error {
	m.holder.Lock()
	defer m.holder.Unlock()

	streamInfo := ExtractStreamInfo(&info.Header)
	if streamInfo.Name == "" {
		streamInfo.Name = fmt.Sprintf("stream:%s", info.StreamId)
	}

	m.q.Update(bson.M{"task_id": info.TaskId}, bson.M{"$set": bson.M{
		"runtime.info":    streamInfo,
		"runtime.headers": info.Header,
		"runtime.ts":      getTs(),
	}})

	if err := m.respawnDeadChannels(); err != nil {
		return err
	}

	channelId := crc32.ChecksumIEEE([]byte(streamInfo.Name))
	err := m.ch.Insert(bson.M{
		"_id":       channelId,
		"name":      streamInfo.Name,
		"task_id":   info.TaskId,
		"stream_id": info.StreamId,
		"info":      streamInfo,
		"ts":        getTs() + TOUCH_TIMEOUT,
	})

	if err != nil {
		if mgo.IsDup(err) {
			// зеркальный поток планируем на час позже (для мониторинга доступности)
			m.q.Update(bson.M{"task_id": info.TaskId}, bson.M{"$set": bson.M{
				"ts":         getTs() + 3600,
				"channel_id": channelId,
			}})
			glog.Infof("duplicate hold %d - stream %d, task %d", channelId, info.StreamId, info.TaskId)
			return nil
		} else {
			return err
		}
	}

	m.q.Update(bson.M{"task_id": info.TaskId}, bson.M{"$set": bson.M{"channel_id": channelId}})
	glog.Infof("hold %d - stream %d, task %d", channelId, info.StreamId, info.TaskId)

	result.Success = true
	return nil
}

func (m *Manager) LoopRespawnDeadChannels() {
	for _ = range time.Tick(TOUCH_TIMEOUT * time.Second) {
		m.respawnDeadChannels()
	}
}

func (m *Manager) respawnDeadChannels() error {
	m.respawner.Lock()
	defer m.respawner.Unlock()

	where := bson.M{"ts": bson.M{"$lte": getTs()}}

	// 	// делаем из taskId хеш
	ids := []uint32{}
	iter := m.ch.Find(where).Select(bson.M{"_id": 1}).Iter()
	var c Channel

	for iter.Next(&c) {
		ids = append(ids, c.Id)
	}
	if err := iter.Close(); err != nil {
		return err
	}

	if len(ids) > 0 {
		glog.Infof("respawn dead channels: %v", ids)

		// поднимаем в очередь зеркальные потоки
		// нерабочии потоки (retry state) пропускаем
		m.q.UpdateAll(bson.M{"channel_id": bson.M{"$in": ids}, "retry_ivl": 0}, bson.M{"$set": bson.M{"ts": getTs()}})

		// удаляем протухшие блокировки
		m.ch.RemoveAll(where)
	}

	return nil
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
