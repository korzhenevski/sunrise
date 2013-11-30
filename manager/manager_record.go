package manager

import (
	"path"
	"strconv"
	"time"
)

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

func GetRecordPath(recordId uint32, baseDir string) string {
	id := strconv.Itoa(int(recordId))
	return path.Join(baseDir, id[len(id)-1:], id+".mp3")
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
