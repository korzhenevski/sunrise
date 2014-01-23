package radio

import (
	"github.com/outself/sunrise/http2"
	. "launchpad.net/gocheck"
	"testing"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestExtractInfo(c *C) {
	h := &http2.Header{}
	h.Add("icy-name", "name ")
	h.Add("icy-br", "192, 128000")
	h.Add("icy-url", "http://www.example.com/")
	h.Add("icy-metaint", " 8019")
	h.Add("icy-pub", "0")

	info := StreamInfo{
		Name:    "name",
		Url:     "http://www.example.com/",
		Bitrate: 192,
		Metaint: 8019,
		Private: true,
	}

	c.Assert(*ExtractInfo(h), Equals, info)
}
