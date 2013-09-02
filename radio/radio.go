package radio

import (
	"bufio"
	"github.com/outself/sunrise/http2"
	"io"
	"log"
	"net"
	"strconv"
	"time"
)

type Radio struct {
	Url    string
	Client *http2.Client
}

type Stream struct {
	res     *http2.Response
	reader  *bufio.Reader
	Metaint int
}

type Chunk struct {
	Data []byte
	Meta string
}

func TimeoutDialer(timeout time.Duration) func(net, addr string) (net.Conn, error) {
	return func(netw, addr string) (net.Conn, error) {
		conn, err := net.DialTimeout(netw, addr, timeout)
		if err != nil {
			return nil, err
		}
		return conn, nil
	}
}

func NewRadio(url string) (stream *Stream, err error) {
	client := &http2.Client{
		Transport: &http2.Transport{
			Dial:                  TimeoutDialer(10 * time.Second),
			DisableKeepAlives:     true,
			MaxIdleConnsPerHost:   -1,
			ResponseHeaderTimeout: 5 * time.Second,
		},
	}
	radio := &Radio{Url: url, Client: client}
	return radio.Get()
}

func (r *Radio) Get() (*Stream, error) {
	var err error
	req, _ := http2.NewRequest("GET", r.Url, nil)

	req.Header.Set("Icy-Metadata", "1")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_4) Chrome/29.0.1547.57 Safari/537.36")

	stream := new(Stream)

	stream.res, err = r.Client.Do(req)
	if err != nil {
		panic(err)
	}

	stream.Metaint, err = strconv.Atoi(stream.res.Header.Get("Icy-Metaint"))
	if err != nil {
		panic("invalid metaint")
	}

	stream.reader = bufio.NewReader(stream.res.Body)
	return stream, nil
}

// Read audio data and metadata from radio stream
func (s *Stream) ReadChunk() (chunk *Chunk, err error) {
	chunk = new(Chunk)
	chunk.Data = make([]byte, s.Metaint)

	_, e := io.ReadFull(s.reader, chunk.Data)
	if e != nil {
		if e == io.EOF {
			return nil, io.EOF
		} else {
			log.Panic(e)
		}
	}

	metabuf, e := s.reader.ReadByte()
	if e != nil {
		log.Panic(e)
	}

	metalen := int(metabuf)
	if metalen > 0 {
		metalen *= 16

		metabuf := make([]byte, metalen)
		io.ReadFull(s.reader, metabuf)

		chunk.Meta = string(metabuf)
	}

	return chunk, nil
}

func (s *Stream) Header() *http2.Header {
	return &s.res.Header
}

func (s *Stream) Close() {
	s.res.Body.Close()
}
