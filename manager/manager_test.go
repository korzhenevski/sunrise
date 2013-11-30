package manager

import (
	"github.com/outself/sunrise/http2"
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

// add failing tests
func (s *MySuite) TestPutAndGetTask(c *C) {
	task := Task{
		StreamUrl:        "http://stream.again.fm:8080/stream01?user=hello%20world",
		StreamId:         1488,
		ServerId:         100,
		Record:           true,
		RecordDuration:   300,
		MinRetryInterval: 10,
	}

	var res PutResult
	var err error
	err = s.m.PutTask(task, &res)
	c.Assert(err, IsNil)
	c.Check(res.Success, Equals, true)

	if res.QueueId == 0 {
		c.Error("Result TaskId is zero")
	}

	t := new(Task)
	err = s.m.GetTask(res.QueueId, t)
	c.Assert(err, IsNil)
	c.Assert(t.Success, Equals, true)

	// explicit checking
	task.QueueId = res.QueueId
	task.Success = true

	c.Check(t, DeepEquals, &task)
}

func (s *MySuite) putTask(streamId uint32, url string, serverId uint32) error {
	task := Task{
		StreamUrl:        url,
		StreamId:         streamId,
		ServerId:         serverId,
		Record:           true,
		RecordDuration:   300,
		MinRetryInterval: 10,
	}

	var res PutResult
	return s.m.PutTask(task, &res)
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

// func (s *MySuite) TestRemoveStream(c *C) {
// 	req := PutRequest{
// 		Id:       1488,
// 		Url:      "http://www.example.com/",
// 		ServerId: 100,
// 	}

// 	var res PutResult
// 	var err error
// 	err = s.m.PutStream(req, &res)
// 	c.Check(err, IsNil)

// 	result := new(OpResult)
// 	err = s.m.RemoveTask(res.TaskId, result)
// 	c.Check(err, IsNil)

// 	result = new(OpResult)
// 	err = s.m.RemoveTask(255, result)
// 	c.Check(err, IsNil)
// 	c.Check(result.Success, Equals, false)
// }

func (s *MySuite) TestTaskNextRetryInterval(c *C) {
	t := &Task{MinRetryInterval: 30, MaxRetryInterval: 40}
	c.Check(t.NextRetryInterval(), Equals, t.MinRetryInterval)
	c.Check(t.NextRetryInterval(), Not(Equals), t.MinRetryInterval)
}

func (s *MySuite) TestTouchTask(c *C) {
	taskId := []uint32{1, 2, 3, 4, 5, 68, 4324234}
	req := TouchRequest{
		TaskId:   taskId,
		ServerId: 10001,
	}
	var res TouchResult
	err := s.m.TouchTask(req, &res)
	c.Assert(err, IsNil)
	c.Assert(res.Success, Equals, true)
	c.Check(res.ObsoleteTaskId, DeepEquals, taskId)
}

func (s *MySuite) TestGenUniqTaskId(c *C) {
	id := genTaskId(100)
	c.Assert(id, Not(Equals), genTaskId(100))
	c.Assert(id, Not(Equals), genTaskId(200))
}

func (s *MySuite) TestExtractStreamInfo(c *C) {
	h := &http2.Header{}
	h.Add("icy-name", "name ")
	h.Add("icy-br", "192, 128000")
	h.Add("icy-url", "http://www.example.com/")
	h.Add("icy-metaint", " 8019")
	h.Add("icy-pub", "0")

	info := SInfo{
		Name:    "name",
		Url:     "http://www.example.com/",
		Bitrate: 192,
		Metaint: 8019,
		Private: true,
	}

	c.Assert(ExtractStreamInfo(h), Equals, info)
}
