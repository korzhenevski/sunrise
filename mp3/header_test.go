package mp3

import (
	"bytes"
	. "launchpad.net/gocheck"
	"testing"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestBaseInfo(c *C) {
	frame, err := GetFirstFrame(bytes.NewReader([]byte{255, 251, 178, 4}))
	c.Assert(err, IsNil)
	c.Check(frame.Bitrate, Equals, 192000)
	c.Check(frame.SampleRate, Equals, 44100)
}
