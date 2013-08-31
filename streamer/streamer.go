package main

import (
	"flag"
	// "fmt"
	"fmt"
	"github.com/golang/glog"
	"github.com/outself/sunrise/manager"
	"github.com/outself/sunrise/rpc2"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"time"
)

type multiFileReader struct {
	readers []io.Reader
}

func (mr *multiFileReader) Read(p []byte) (n int, err error) {
	for len(mr.readers) > 0 {
		n, err = mr.readers[0].Read(p)
		if n > 0 || err != io.EOF {
			if err == io.EOF {
				// Don't return EOF yet. There may be more bytes
				// in the remaining readers.
				err = nil
			}
			return
		}
		mr.readers = mr.readers[1:]
	}
	return 0, io.EOF
}

// MultiReader returns a Reader that's the logical concatenation of
// the provided input readers.  They're read sequentially.  Once all
// inputs are drained, Read will return EOF.
func MultiFileReader(files []string) io.Reader {
	mr := multiFileReader{make([]io.Reader, len(files))}
	for _, name := range files {
		f, err := os.Open(name)
		if err != nil {
			glog.Errorln(err)
		} else {
			mr.readers = append(mr.readers, f)
		}
	}
	return &mr
}

// START_1 OMIT
// A SizeReaderAt is a ReaderAt with a Size method.
//
// An io.SectionReader implements SizeReaderAt.
type SizeReaderAt interface {
	Size() int64
	io.ReaderAt
}

// NewMultiReaderAt is like io.MultiReader but produces a ReaderAt
// (and Size), instead of just a reader.
func NewMultiReaderAt(parts ...SizeReaderAt) SizeReaderAt {
	m := &multi{
		parts: make([]offsetAndSource, 0, len(parts)),
	}
	var off int64
	for _, p := range parts {
		m.parts = append(m.parts, offsetAndSource{off, p})
		off += p.Size()
	}
	m.size = off
	return m
}

// END_1 OMIT

type offsetAndSource struct {
	off int64
	SizeReaderAt
}

type multi struct {
	parts []offsetAndSource
	size  int64
}

func (m *multi) Size() int64 { return m.size }

func (m *multi) ReadAt(p []byte, off int64) (n int, err error) {
	wantN := len(p)

	// Skip past the requested offset.
	skipParts := sort.Search(len(m.parts), func(i int) bool {
		// This function returns whether parts[i] will
		// contribute any bytes to our output.
		part := m.parts[i]
		return part.off+part.Size() > off
	})
	parts := m.parts[skipParts:]

	// How far to skip in the first part.
	needSkip := off
	if len(parts) > 0 {
		needSkip -= parts[0].off
	}

	for len(parts) > 0 && len(p) > 0 {
		readP := p
		partSize := parts[0].Size()
		if int64(len(readP)) > partSize-needSkip {
			readP = readP[:partSize-needSkip]
		}
		pn, err0 := parts[0].ReadAt(readP, needSkip)
		if err0 != nil {
			return n, err0
		}
		n += pn
		p = p[pn:]
		if int64(pn)+needSkip == partSize {
			parts = parts[1:]
		}
		needSkip = 0
	}

	if n != wantN {
		err = io.ErrUnexpectedEOF
	}
	return
}

type streamer struct {
	rpc *rpc2.Client
}

func Streamer(rpc *rpc2.Client) http.Handler {
	return &streamer{rpc}
}

func getFormInt(key string, rw http.ResponseWriter, r *http.Request) uint64 {
	i, err := strconv.ParseUint(r.FormValue(key), 10, 0)
	if err != nil {
		http.Error(rw, err.Error(), 400)
	}
	return i
}

func (s *streamer) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	streamId := getFormInt("sid", rw, r)
	ts := getFormInt("ts", rw, r)

	t := time.Unix(int64(ts), 0)
	req := manager.RecordRequest{StreamId: uint32(streamId), From: t, To: t.Add(84600 * time.Second)}
	reply := new(manager.RecordReply)
	err := s.rpc.Call("Tracker.GetRecord", req, reply)
	if err != nil {
		http.Error(rw, err.Error(), 500)
	}

	files := make([]SizeReaderAt, 0)
	size := uint32(0)
	for _, record := range reply.Record {
		files = append(files, RecordFile(&record))
		size += record.Size
		fmt.Printf("%d %s %s\n", record.Id, record.Path, time.Unix(int64(record.Time), 0))
	}
	fmt.Printf("total size: %d\n", size)
	sra := NewMultiReaderAt(files...)
	rs := io.NewSectionReader(sra, 0, sra.Size())

	//fmt.Fprintf(rw, "Hello, stream_id: %d, ts: %d", streamId, ts)
	http.ServeContent(rw, r, fmt.Sprintf("%d_%d.mp3", streamId, ts), t, rs)
	// http.ServeFile(rw, r, reply.Record[0].Path)
}

type recordFile struct {
	file   *os.File
	record *manager.Record
}

func (r *recordFile) ReadAt(b []byte, off int64) (int, error) {
	return r.file.ReadAt(b, off)
}

func (r *recordFile) Size() int64 {
	return int64(r.record.Size)
}

func RecordFile(record *manager.Record) *recordFile {
	var err error
	rf := &recordFile{record: record}
	rf.file, err = os.Open(record.Path)
	if err != nil {
		panic(err)
	}
	return rf
}

func main() {
	tracker := flag.String("tracker", "localhost:4242", "rpc-tracker address")
	flag.Parse()
	defer glog.Flush()

	rpc := rpc2.NewClient(*tracker)
	rpc.Dial()

	http.Handle("/s", Streamer(rpc))
	glog.Fatal(http.ListenAndServe(":8080", nil))
}
