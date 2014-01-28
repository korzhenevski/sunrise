package backend

import (
	"github.com/golang/glog"
	"github.com/outself/sunrise/radio"
	"hash/crc32"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"net/rpc"
	"time"
)

type StreamService struct {
	db          *mgo.Database
	bd          *rpc.Client
	streams     *mgo.Collection
	stream_info *mgo.Collection
	playlists   *mgo.Collection
}

func NewStreamService(db *mgo.Database, bd *rpc.Client) *StreamService {
	service := &StreamService{
		db:          db,
		bd:          bd,
		streams:     db.C("streams"),
		stream_info: db.C("streaminfo"),
		playlists:   db.C("playlists"),
	}

	// Indexes
	// streams [owner_id, url]
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

type StreamInfo struct {
	Url  string           `bson:"_id"`
	Info radio.StreamInfo `bson:"info"`
}

type Stream struct {
	Id        int    `bson:"_id"`
	Name      string `bson:"name,omitempty"`
	RadioId   int    `bson:"radio_id,omitempty"`
	OwnerId   int    `bson:"owner_id,omitempty"`
	DeletedAt int64  `bson:"deleted_at,minsize"`
	AddedAt   int64  `bson:"added_at,omitempty,minsize"`
	Url       string `bson:"url"`
	// info from headers
	Info radio.StreamInfo `bson:"info"`
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

	iter := s.streams.Find(where).Limit(1000).Iter()
	if err := iter.All(&result.Items); err != nil {
		return err
	}
	return nil
}

func (s *StreamService) Save(stream Stream, result *int) error {
	var err error
	if stream.Id, err = NextId(s.db, "streams"); err != nil {
		return err
	}
	if stream.Url, err = normalizeUrl(stream.Url); err != nil {
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

	// go func(stream Stream) {
	// TODO: мету важно получить сразу,
	// пока терпимо без фоновой обработки
	if err := s.updateInfo(stream); err != nil {
		glog.Infof("update_info: %s - %s", stream.Url, err)
	}
	// }(stream)

	*result = stream.Id
	return nil
}

func (s *StreamService) updateInfo(stream Stream) error {
	var info radio.StreamInfo
	// TODO: можно вынести в отдельный сервис и дергать по RPC
	if err := s.FetchInfo(StreamFetchInfoParams{stream.Url}, &info); err != nil {
		glog.Warningf("fetch info err: %s", err)
		return err
	}

	radioId := crc32.ChecksumIEEE([]byte(info.Name))
	glog.Infof("update_info: %s - info=%+v radio_id=%d", stream.Url, info, radioId)

	if err := s.streams.UpdateId(stream.Id, bson.M{"$set": bson.M{"radio_id": radioId, "info": info}}); err != nil {
		glog.Warningf("stream %d update err: %s", stream.Id, err)
		return err
	}

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

func (s *StreamService) Delete(params StreamDeleteParams, response *bool) error {
	// soft-delete: set current timestamp to deleted_at
	update := bson.M{"$set": bson.M{"deleted_at": time.Now().Unix()}}
	if err := s.streams.Update(bson.M{"_id": params.RadioId, "owner_id": params.OwnerId}, update); err != nil {
		return err
	}
	return nil
}

type StreamGetListenParams struct {
	RadioId int
	OwnerId int
	Bitrate int
}

type StreamListen struct {
	Bitrate int      `bson:"_id"`
	Url     []string `bson:"url"`
}

type StreamGetListenResult struct {
}

func (s *StreamService) GetListen(params StreamGetListenParams, result *StreamListen) error {
	where := bson.M{"owner_id": params.OwnerId, "radio_id": params.RadioId}
	iter := s.streams.Pipe([]bson.M{
		{"$match": where},
		{"$group": bson.M{"_id": "$info.bitrate", "url": bson.M{"$addToSet": "$url"}}},
	})
	if err := iter.One(&result); err != nil {
		return err
	}
	return nil
}

type StreamGetChannelsParams struct {
	OwnerId int
}

type StreamChannel struct {
	Title       string   `bson:"_id"`
	RadioId     int      `bson:"radio_id"`
	Bitrate     []int    `bson:"bitrate"`
	StreamId    []int    `bson:"stream_id"`
	ContentType []string `bson:"content_type"`
}

type ChannelsResult struct {
	Items []StreamChannel
}

func (s *StreamService) GetChannels(params StreamGetChannelsParams, result *ChannelsResult) error {
	iter := s.streams.Pipe([]bson.M{
		{"$match": bson.M{"owner_id": params.OwnerId, "radio_id": bson.M{"$gt": 0}}},
		{"$project": bson.M{
			"radio_id":     1,
			"stream_id":    "$_id",
			"title":        "$info.name",
			"bitrate":      "$info.bitrate",
			"content_type": "$info.content_type",
		}},
		{"$group": bson.M{
			"_id":          "$title",
			"radio_id":     bson.M{"$first": "$radio_id"},
			"bitrate":      bson.M{"$addToSet": "$bitrate"},
			"stream_id":    bson.M{"$addToSet": "$stream_id"},
			"content_type": bson.M{"$addToSet": "$content_type"},
		}},
	})

	if err := iter.All(&result.Items); err != nil {
		return err
	}

	return nil
}

type StreamSearchParams struct {
	Query string
}

func (s *StreamService) Search(params StreamSearchParams, result *StreamResult) error {
	return nil
}

type StreamFetchInfoParams struct {
	Url string
}

func (s *StreamService) FetchInfo(params StreamFetchInfoParams, result *radio.StreamInfo) error {
	// caching
	var info StreamInfo
	if err := s.stream_info.FindId(params.Url).One(&info); err != nil {
		if err != mgo.ErrNotFound {
			return err
		}
	} else {
		*result = info.Info
		return nil
	}

	// new request
	r, err := radio.NewRadioWithTimeout(params.Url, "WinampMPEG/5.0", 5*time.Second)
	if err != nil {
		return err
	}
	defer r.Close()
	*result = *radio.ExtractInfo(r.Header())

	// update cache
	if _, err := s.stream_info.UpsertId(params.Url, StreamInfo{Url: params.Url, Info: *result}); err != nil {
		return err
	}
	return nil
}
