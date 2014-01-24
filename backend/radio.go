package backend

import (
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"time"
)

type RadioService struct {
	db    *mgo.Database
	radio *mgo.Collection
}

func NewRadioService(db *mgo.Database) *RadioService {
	return &RadioService{
		db:    db,
		radio: db.C("radio"),
	}
}

type Radio struct {
	Id        int   `bson:"_id"`
	OwnerId   int   `bson:"owner_id,omitempty"`
	DeletedAt int64 `bson:"deleted_at,minsize"`
	AddedAt   int64 `bson:"added_at,omitempty,minsize"`
	Title     string
}

type RadioGetParams struct {
	OwnerId int
	Id      []int
}

type RadioResult struct {
	Items []Radio
}

func (r *RadioService) Get(params RadioGetParams, result *RadioResult) error {
	where := bson.M{"deleted_at": 0}
	if params.OwnerId > 0 {
		where["owner_id"] = params.OwnerId
	}

	if len(params.Id) > 0 {
		where["_id"] = bson.M{"$in": params.Id}
	}

	it := r.radio.Find(where).Limit(100).Iter()
	if err := it.All(&result.Items); err != nil {
		return err
	}

	return nil
}

func (r *RadioService) Add(radio Radio, result *int) error {
	var err error
	if radio.Id, err = NextId(r.db, "radio"); err != nil {
		return err
	}
	radio.AddedAt = time.Now().Unix()

	if err := r.radio.Insert(radio); err != nil {
		return err
	}

	*result = radio.Id
	return nil
}

func (r *RadioService) Edit(radio Radio, result *int) error {
	if err := r.radio.Update(bson.M{"_id": radio.Id, "owner_id": radio.OwnerId}, radio); err != nil {
		return err
	}
	*result = radio.Id
	return nil
}

type RadioDeleteParams struct {
	OwnerId int
	RadioId int
}

func (r *RadioService) Delete(params *RadioDeleteParams, response *bool) error {
	// soft-delete: set current timestamp to deleted_at
	update := bson.M{"$set": bson.M{"deleted_at": time.Now().Unix()}}
	if err := r.radio.Update(bson.M{"_id": params.RadioId, "owner_id": params.OwnerId}, update); err != nil {
		return err
	}
	return nil
}

type RadioSearchParams struct {
	Query string
}

func (r *RadioService) Search(params RadioSearchParams, result *RadioResult) error {
	return nil
}
