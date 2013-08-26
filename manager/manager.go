package manager

import (
	"errors"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log"
	"math/rand"
	"path"
	"strconv"
	"time"
)

// add to replicaCommit: record length, ID3, md5
// add interface for add/remove Server, Volume
// http://zookeeper.apache.org/doc/trunk/zookeeperTutorial.html#sc_producerConsumerQueues

// Listen records from any point
// Durable worker
// Extract queue specific logic to Worker class
// Test with 1024 streams

type Manager struct {
	db      *mgo.Database
	q       *mgo.Collection
	air     *mgo.Collection
	ids     *mgo.Collection
	records *mgo.Collection
	servers *mgo.Collection
	volumes *mgo.Collection
}

// TODO
// Get air list (q: from, to)
// Serve air by HTTP (nginx)
// Task backoff retry, retry logging

func New(db *mgo.Database) *Manager {
	manager := &Manager{}

	manager.db = db
	manager.q = db.C("queue")
	manager.air = db.C("air")
	manager.ids = db.C("ids")
	manager.records = db.C("records")
	manager.servers = db.C("servers")
	manager.volumes = db.C("volumes")

	return manager
}

type Task struct {
	Id        uint32 `bson:"_id"`
	StreamUrl string `bson:"url"`
	StreamId  uint32 `bson:"stream_id"`
	ServerId  uint32 `bson:"server_id"`
	Success   bool
}

type TrackMeta struct {
	TaskId   uint32
	StreamId uint32
	RecordId uint32
	Checksum uint32
	Meta     string
	Duration uint32
	PrevId   uint32
}

type TrackResult struct {
	MetaId      uint32
	RecordId    uint32
	RecordPath  string
	MaxDuration uint32
	Success     bool
}

type Air struct {
	Id       uint32 `bson:"_id"`
	StreamId uint32 `bson:"sid"`
	Title    string `bson:"t"`
	Time     uint32 `bson:"ts"`
	EndTime  uint32 `bson:"ets"`
	RecordId uint32 `bson:"rid"`
	PrevId   uint32 `bson:"pid"`
	Ended    bool   `bson:"end"`
}

type RecordReplica struct {
	ServerId     uint32 `bson:"sid"`
	VolumeId     uint32 `bson:"vid"`
	Time         uint32 `bson:"ts"`
	ProcessTime  uint32 `bson:"pts"`
	ReplicatedAt uint32 `bson:"rts"`
}

type Record struct {
	Id        uint32          `bson:"_id"`
	ServerId  uint32          `bson:"sid"`
	VolumeId  uint32          `bson:"vid"`
	Checksum  uint32          `bson:"csum"`
	Size      uint32          `bson:"size"`
	Time      uint32          `bson:"ts"`
	EndTime   uint32          `bson:"ets"`
	DeletedAt uint32          `bson:"dts"`
	Ended     bool            `bson:"end"`
	Replica   []RecordReplica `bson:"r"`
}

type Replica struct {
	Url      string
	VolumeId uint32
	Path     string
	RecordId uint32
	Success  bool
}

type ReplicaCommit struct {
	RecordId uint32
	VolumeId uint32
}

type CommitResult struct {
	Success bool
}

type Volume struct {
	Id       uint32 `bson:"_id"`
	ServerId uint32 `bson:"server_id"`
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

type TouchResult struct {
	Success bool
}

func GetRecordPath(recordId uint32, baseDir string) string {
	id := strconv.Itoa(int(recordId))
	return path.Join(baseDir, id[0:1], id+".mp3")
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

func (m *Manager) getTask(taskId uint32, task *Task) (err error) {
	err = m.q.Find(bson.M{"_id": taskId}).One(task)
	if err != nil {
		return
	}
	return nil
}

func (m *Manager) TrackMeta(data TrackMeta, result *TrackResult) error {
	ts := uint32(time.Now().Unix())
	title := ExtractStreamTitle(data.Meta)

	log.Printf(">>>> %+v", data)

	var task Task
	err := m.q.Find(bson.M{"_id": data.TaskId}).One(&task)
	if err == mgo.ErrNotFound {
		result.Success = false
		return nil
	} else if err != nil {
		return err
	}

	log.Printf("~~~~ %+v", task)

	// generate new id
	id, err := m.nextId("air")
	if err != nil {
		return err
	}

	uploadVolume, err := m.selectUploadVolume(task.ServerId)
	if err != nil {
		return err
	}

	record, err := m.newRecord(task.ServerId, uploadVolume.Id)
	if err != nil {
		return err
	}

	result.RecordId = record.Id
	result.RecordPath = GetRecordPath(record.Id, uploadVolume.Path)
	result.MetaId = id
	// TODO: value from task?
	result.MaxDuration = 30

	// save air record
	air := &Air{
		Id:       id,
		StreamId: data.StreamId,
		Title:    title,
		Time:     ts,
		EndTime:  0,
		RecordId: result.RecordId,
		PrevId:   data.PrevId,
		Ended:    false}

	if err := m.air.Insert(air); err != nil {
		return err
	}

	// end previous
	if data.PrevId != 0 {
		// end air
		change := bson.M{"$set": bson.M{"end": true, "ets": ts, "duration": data.Duration}}
		if err := m.air.UpdateId(data.PrevId, change); err != nil {
			return err
		}

		if data.RecordId > 0 {
			m.records.UpdateId(data.RecordId, bson.M{"$set": bson.M{"end": true, "ets": ts, "csum": data.Checksum}})
			if err != nil {
				return err
			}
			m.replicateRecord(data.RecordId, task.ServerId)
		}

		log.Printf("%d ended", data.PrevId)
	}

	result.Success = true
	log.Printf("<<<< %+v", result)
	return nil
}

func (m *Manager) replicateRecord(recordId uint32, serverId uint32) error {
	vol, err := m.selectReplicaVolume(serverId)
	if err != nil {
		return err
	}
	replica := &RecordReplica{ServerId: vol.ServerId, VolumeId: vol.Id, Time: uint32(time.Now().Unix()), ReplicatedAt: 0}
	log.Printf("REPLICA: %+v", replica)
	err = m.records.UpdateId(recordId, bson.M{"$push": bson.M{"r": replica}})
	if err != nil {
		return err
	}
	return nil
}

func (m *Manager) selectUploadVolume(serverId uint32) (*Volume, error) {
	volume := new(Volume)
	err := m.volumes.Find(bson.M{"server_id": serverId, "online": true, "upload": true}).One(&volume)
	if err == mgo.ErrNotFound {
		return nil, errors.New("no upload volume")
	} else if err != nil {
		return nil, err
	}

	return volume, nil
}

func (m *Manager) selectReplicaVolume(serverId uint32) (*Volume, error) {
	var result []Volume
	// select any online volume not in source server
	err := m.volumes.Find(bson.M{"server_id": bson.M{"$ne": serverId}, "online": true}).Iter().All(&result)

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

func (m *Manager) ReserveTask(serverId uint32, task *Task) error {
	ts := time.Now().Unix()

	where := bson.M{"ts": bson.M{"$lt": ts - 10}}
	change := mgo.Change{
		Update:    bson.M{"$set": bson.M{"ts": ts, "server_id": serverId}},
		ReturnNew: true,
	}
	_, err := m.q.Find(where).Apply(change, task)
	if err == mgo.ErrNotFound {
		// log.Println("no tasks")
		return nil
	} else if err != nil {
		return err
	}

	task.Success = true
	return nil
}

func (m *Manager) TouchTask(taskId uint32, res *TouchResult) error {
	//log.Printf("touch task %d", taskId)

	err := m.q.UpdateId(taskId, bson.M{"$set": bson.M{"ts": time.Now().Unix()}})
	if err == mgo.ErrNotFound {
		log.Printf("task %d not found", taskId)
		return nil
	} else if err != nil {
		return err
	}

	res.Success = true
	return nil
}

func (m *Manager) RequestReplica(serverId uint32, replica *Replica) error {
	ts := time.Now().Unix()

	where := bson.M{"r.sid": serverId, "r.rts": 0, "r.pts": bson.M{"$lt": ts - 10}}
	change := mgo.Change{
		Update:    bson.M{"$set": bson.M{"r.$.pts": ts}},
		ReturnNew: true,
	}
	record := new(Record)
	_, err := m.records.Find(where).Apply(change, &record)
	if err == mgo.ErrNotFound {
		log.Println("not found")
		return nil
	} else if err != nil {
		return err
	}
	log.Printf("%+v", record)

	var server Server
	if err := m.servers.FindId(record.ServerId).One(&server); err != nil {
		log.Printf("record %d, server %d not found", record.Id, record.ServerId)
		return err
	}

	var vol Volume
	if err := m.volumes.FindId(record.Replica[0].VolumeId).One(&vol); err != nil {
		log.Printf("record %d, server %d not found", record.Id, record.VolumeId)
		return err
	}

	replica.Url = server.GetRecordUrl(record)
	replica.VolumeId = vol.Id
	replica.Path = GetRecordPath(record.Id, vol.Path)
	replica.RecordId = record.Id
	replica.Success = true

	return nil
}

func (m *Manager) CommitReplica(commit ReplicaCommit, res *CommitResult) error {
	log.Printf(">>>> %+v", commit)
	ts := uint32(time.Now().Unix())
	err := m.records.Update(bson.M{"_id": commit.RecordId, "r.vid": commit.VolumeId}, bson.M{"$set": bson.M{"r.$.rts": ts}})
	if err == mgo.ErrNotFound {
		log.Println("not found")
		return nil
	} else if err != nil {
		return err
	}
	res.Success = true
	return nil
}
