package backend

import (
	"labix.org/v2/mgo"
	. "launchpad.net/gocheck"
	"log"
	"testing"
	"time"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

var _ = Suite(&MySuite{})

type MySuite struct {
	db *mgo.Database
	s  *StreamService
}

func (s *MySuite) SetUpSuite(c *C) {
	session, err := mgo.DialWithTimeout("localhost", 1*time.Second)
	if err != nil {
		log.Fatal("mongo dial error:", err)
	}
	s.db = session.DB("bd_stream_test")
	s.s = NewStreamService(s.db)
}

func (s *MySuite) TearDownSuite(c *C) {
	if err := s.db.DropDatabase(); err != nil {
		log.Println(err)
	}
}

func (s *MySuite) TestStreamSave(c *C) {
	stream := Stream{
		RadioId: 100,
		OwnerId: 200,
		Url:     "http://stream.example.com/",
	}
	streamId := new(int)
	err := s.s.Save(stream, streamId)
	c.Assert(err, IsNil)
	c.Check(streamId, Not(Equals), 0)
}
