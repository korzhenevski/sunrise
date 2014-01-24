package backend

import (
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	// "log"
)

type nextId struct {
	Next int
}

func NextId(db *mgo.Database, objectType string) (int, error) {
	update := mgo.Change{
		Update:    bson.M{"$inc": bson.M{"next": 1}},
		Upsert:    true,
		ReturnNew: true,
	}

	var result nextId
	if _, err := db.C("ids").Find(bson.M{"_id": objectType}).Apply(update, &result); err != nil {
		return -1, err
	}

	return result.Next, nil
}
