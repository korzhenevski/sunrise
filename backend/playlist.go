package backend

import (
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"time"
)

type PlaylistService struct {
	db       *mgo.Database
	playlist *mgo.Collection
}

func NewPlaylistService(db *mgo.Database) *PlaylistService {
	return &PlaylistService{
		db:       db,
		playlist: db.C("playlist"),
	}
}

type Playlist struct {
	Id      int      `bson:"_id"`
	Content string   `bson:"content"`
	OwnerId int      `bson:"owner_id"`
	Streams []string `bson:"streams"`
	AddedAt int64    `bson:"added_at,omitempty,minsize"`
}

type PlaylistResult struct {
	Items []Playlist
}

type PlaylistAddParams struct {
	Id      int
	Content string
	OwnerId int
}

func (p *PlaylistService) Add(params PlaylistAddParams, result *int) error {
	playlist := &Playlist{}
	var err error
	if playlist.Id, err = NextId(p.db, "playlist"); err != nil {
		return err
	}

	playlist.OwnerId = params.OwnerId
	playlist.Streams = uniqUrls(parsePlaylist(params.Content))
	playlist.AddedAt = time.Now().Unix()

	if err := p.playlist.Insert(playlist); err != nil {
		return err
	}

	*result = playlist.Id
	return nil
}

type PlaylistGetParams struct {
	OwnerId int
	Id      []int
}

func (p *PlaylistService) Get(params PlaylistGetParams, result *PlaylistResult) error {
	where := bson.M{}
	if params.OwnerId > 0 {
		where["owner_id"] = params.OwnerId
	}

	if len(params.Id) > 0 {
		where["_id"] = bson.M{"$in": params.Id}
	}

	it := p.playlist.Find(where).Limit(100).Iter()
	if err := it.All(&result.Items); err != nil {
		return err
	}
	return nil
}
