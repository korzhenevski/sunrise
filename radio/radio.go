package radio

import (
	"bufio"
	"errors"
	"github.com/fiam/gounidecode/unidecode"
	"github.com/outself/sunrise/http2"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

var ErrInvalidMetaint = errors.New("invalid stream metaint")

type Radio struct {
	Url       string
	Client    *http2.Client
	UserAgent string
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

func NewRadio(url string, userAgent string) (stream *Stream, err error) {
	client := &http2.Client{
		Transport: &http2.Transport{
			Dial:                  TimeoutDialer(10 * time.Second),
			DisableKeepAlives:     true,
			MaxIdleConnsPerHost:   -1,
			ResponseHeaderTimeout: 5 * time.Second,
		},
	}
	radio := &Radio{Url: url, Client: client, UserAgent: userAgent}
	return radio.Get()
}

func (r *Radio) Get() (*Stream, error) {
	var err error
	req, _ := http2.NewRequest("GET", r.Url, nil)

	req.Header.Set("Icy-Metadata", "1")
	req.Header.Set("User-Agent", r.UserAgent)

	st := new(Stream)

	st.res, err = r.Client.Do(req)
	if err != nil {
		return nil, err
	}

	if st.res.StatusCode != http2.StatusOK {
		return nil, errors.New("http error " + st.res.Status)
	}

	st.Metaint, err = strconv.Atoi(st.res.Header.Get("Icy-Metaint"))
	if err != nil {
		return nil, ErrInvalidMetaint
	}

	st.reader = bufio.NewReader(st.res.Body)
	return st, nil
}

// Read audio data and metadata from radio stream
func (s *Stream) ReadChunk() (chunk *Chunk, err error) {
	chunk = new(Chunk)
	chunk.Data = make([]byte, s.Metaint)

	if _, e := io.ReadFull(s.reader, chunk.Data); e != nil {
		return nil, e
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

		chunk.Meta = strings.TrimRight(string(metabuf), "\x00")
		if !utf8.ValidString(chunk.Meta) {
			chunk.Meta = FixMeta(chunk.Meta)
		}
	}

	return chunk, nil
}

func (s *Stream) Header() *http2.Header {
	return &s.res.Header
}

func (s *Stream) Close() {
	s.res.Body.Close()
}

// нормализация умляутов и прочего юникода
// TODO: потенциально заменить на go-text/unicode/norm
func FixMeta(meta string) string {
	return unidecode.Unidecode(meta)
}
