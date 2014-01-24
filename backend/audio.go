package backend

import (
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"time"
)

type AudioService struct {
	db    *mgo.Database
	audio *mgo.Collection
}

type Audio struct {
	Id       int    `bson:"_id"`
	RadioId  int    `bson:"radio_id"`
	Title    string `bson:"title"`
	Duration int    `bson:"duration"`
	Date     int64  `bson:"ts"`
}

type AudioGetParams struct {
	RadioId int
	Day     string
}

type AudioResult struct {
	Items []Audio
}

const dayFormat = "2006-01-02"

func (a *AudioService) Get(params AudioGetParams, result *AudioResult) error {
	where := bson.M{"radio_id": params.RadioId}

	// filter by day
	if len(params.Day) > 0 {
		day, err := time.Parse(dayFormat, params.Day)
		if err != nil {
			return err
		}
		dayTo := day.AddDate(0, 0, 1)
		where["ts"] = bson.M{"$gte": day.Unix(), "$lt": dayTo.Unix()}
	}

	if err := a.audio.Find(where).Limit(1000).All(&result.Items); err != nil {
		return err
	}

	return nil
}

// func (a *AudioService) Search() {

// }

func NewAudioService(db *mgo.Database) *AudioService {
	return &AudioService{
		db:    db,
		audio: db.C("air"),
	}
}
