package manager

import (
	"labix.org/v2/mgo"
	. "launchpad.net/gocheck"
	"log"
	"testing"
	"time"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type MySuite struct {
	db *mgo.Database
	m  *Manager
}

func (s *MySuite) SetUpSuite(c *C) {
	session, err := mgo.DialWithTimeout("localhost", 1*time.Second)
	if err != nil {
		log.Fatal("mongo dial error:", err)
	}
	s.db = session.DB("manager-testing")
	// current package New
	s.m = New(s.db)
}

func (s *MySuite) TearDownSuite(c *C) {
	if err := s.db.DropDatabase(); err != nil {
		log.Println(err)
	}
}

var _ = Suite(&MySuite{})

func (s *MySuite) TestNextId(c *C) {
	var nid uint32
	nid, _ = s.m.nextId("test")
	c.Assert(nid, Equals, uint32(1))

	nid, _ = s.m.nextId("test")
	c.Assert(nid, Equals, uint32(2))
}

func (s *MySuite) TestPutAndGetStream(c *C) {
	req := PutRequest{
		Url:            "http://stream.again.fm:8080/stream01?user=hello%20world",
		Id:             1488,
		ServerId:       100,
		Record:         true,
		RecordDuration: 300,
	}

	var res PutResult
	var err error
	err = s.m.PutStream(req, &res)

	c.Check(err, Equals, nil)
	c.Check(res.Success, Equals, true)
	if res.TaskId == 0 {
		c.Error("Result TaskId is zero")
	}

	var task Task
	err = s.m.GetTask(res.TaskId, &task)
	c.Check(err, Equals, nil)

	// explicit checking
	c.Check(task.Id, Equals, res.TaskId)
	c.Check(task.StreamUrl, Equals, req.Url)
	c.Check(task.StreamId, Equals, req.Id)
	c.Check(task.ServerId, Equals, req.ServerId)
	c.Check(task.Record, Equals, req.Record)
	c.Check(task.RecordDuration, Equals, req.RecordDuration)
}

func (s *MySuite) TestNormalizeUrl(c *C) {
	var url string
	var err error
	url, err = NormalizeUrl(" hTTp://example.com/%47%6f%2f")
	c.Check(url, Equals, "http://example.com/Go/")
	c.Assert(err, IsNil)

	url, err = NormalizeUrl("HTTP://test.com/")
	c.Check(url, Equals, "http://test.com/")
	c.Assert(err, IsNil)
}

func (s *MySuite) TestRemoveStream(c *C) {
	req := PutRequest{
		Id:       1488,
		Url:      "http://www.example.com/",
		ServerId: 100,
	}

	var res PutResult
	var err error
	err = s.m.PutStream(req, &res)
	c.Check(err, IsNil)

	result := new(OpResult)
	err = s.m.RemoveTask(res.TaskId, result)
	c.Check(err, IsNil)

	result = new(OpResult)
	err = s.m.RemoveTask(255, result)
	c.Check(err, IsNil)
	c.Check(result.Success, Equals, false)
}

func (s *MySuite) TestTaskNextRetryInterval(c *C) {
	t := &Task{MinRetryInterval: 30, MaxRetryInterval: 3600}
	// c.Check(t.NextRetryInterval(), Equals, 100000)
	//c.Error(t.NextRetryInterval())
}
