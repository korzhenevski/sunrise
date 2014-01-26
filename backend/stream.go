package backend

import (
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log"
	"time"
)

type StreamService struct {
	db        *mgo.Database
	streams   *mgo.Collection
	playlists *mgo.Collection
}

func NewStreamService(db *mgo.Database) *StreamService {
	service := &StreamService{
		db:        db,
		streams:   db.C("streams"),
		playlists: db.C("playlists"),
	}

	idx := mgo.Index{
		Key:      []string{"owner_id", "url"},
		DropDups: true,
		Unique:   true,
	}
	if err := service.streams.EnsureIndex(idx); err != nil {
		panic(err)
	}

	return service
}

type Stream struct {
	Id        int   `bson:"_id"`
	RadioId   int   `bson:"radio_id,omitempty"`
	OwnerId   int   `bson:"owner_id,omitempty"`
	DeletedAt int64 `bson:"deleted_at,minsize"`
	AddedAt   int64 `bson:"added_at,omitempty,minsize"`
	Url       string
}

type StreamGetParams struct {
	OwnerId int
	RadioId int
	Id      []int
}

type StreamResult struct {
	Items []Stream
}

func (s *StreamService) Get(params StreamGetParams, result *StreamResult) error {
	where := bson.M{"deleted_at": 0}
	if params.OwnerId > 0 {
		where["owner_id"] = params.OwnerId
	}

	if len(params.Id) > 0 {
		where["_id"] = bson.M{"$in": params.Id}
	}

	it := s.streams.Find(where).Limit(100).Iter()
	if err := it.All(&result.Items); err != nil {
		return err
	}

	return nil
}

// type StreamAddParams struct {
// 	OwnerId  int
// 	StreamId int
// }

// func (s *StreamService) Add(params StreamAddParams, result *bool) error {
// 	return nil
// }

func (s *StreamService) Save(stream Stream, result *int) error {
	var err error
	if stream.Id, err = NextId(s.db, "streams"); err != nil {
		return err
	}
	stream.AddedAt = time.Now().Unix()

	if err := s.streams.Insert(stream); err != nil {
		if mgo.IsDup(err) {
			*result = -1
			return nil
		}
		return err
	}

	*result = stream.Id
	return nil
}

func (s *StreamService) Edit(stream Stream, result *int) error {
	if err := s.streams.Update(bson.M{"_id": stream.Id, "owner_id": stream.OwnerId}, stream); err != nil {
		return err
	}
	*result = stream.Id
	return nil
}

type StreamDeleteParams struct {
	OwnerId int
	RadioId int
}

func (s *StreamService) Delete(params *StreamDeleteParams, response *bool) error {
	// soft-delete: set current timestamp to deleted_at
	update := bson.M{"$set": bson.M{"deleted_at": time.Now().Unix()}}
	if err := s.streams.Update(bson.M{"_id": params.RadioId, "owner_id": params.OwnerId}, update); err != nil {
		return err
	}
	return nil
}

// type StreamSearchParams struct {
// 	Query string
// }

// func (s *StreamService) Search(params StreamSearchParams, result *StreamResult) error {
// 	return nil
// }

type StreamUpdateMetaParams struct {
	Playlist string
}

func (s *StreamService) UpdateMeta(params StreamUpdateMetaParams, result *bool) error {
	return nil
}
