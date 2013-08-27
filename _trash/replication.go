package manager

import (
	"errors"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log"
	"math/rand"
	"time"
)

type RecordReplica struct {
	ServerId     uint32 `bson:"sid"`
	VolumeId     uint32 `bson:"vid"`
	Time         uint32 `bson:"ts"`
	ProcessTime  uint32 `bson:"pts"`
	ReplicatedAt uint32 `bson:"rts"`
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
