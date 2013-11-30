package manager

import (
	crypto_rand "crypto/rand"
	"github.com/vova616/xxhash"
	"io"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"time"
)

type NextId struct {
	Next uint32
}

func getTs() uint32 {
	return uint32(time.Now().Unix())
}

func getTouchTs() uint32 {
	return uint32(time.Now().Unix()) + TOUCH_TIMEOUT
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

func genTaskId(workerId uint32) uint32 {
	b := make([]byte, 10)
	_, err := io.ReadFull(crypto_rand.Reader, b)
	if err != nil {
		panic(err)
	}
	return xxhash.Checksum32Seed(b, workerId)
}
