package main

import (
	"flag"
	"github.com/golang/glog"
	"github.com/kr/pretty"
	"github.com/outself/sunrise/manager"
	"github.com/outself/sunrise/rpc2"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

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

func part(s string) SizeReaderAt {
	return io.NewSectionReader(strings.NewReader(s), 0, int64(len(s)))
}

func handler(w http.ResponseWriter, r *http.Request) {
	sra := NewMultiReaderAt(
		part("Hello, "), part(" world! "),
		part("You requested "+r.URL.Path+"\n"),
	)
	rs := io.NewSectionReader(sra, 0, sra.Size())
	http.ServeContent(w, r, "foo.txt", time.Now(), rs)
}

func (s *streamer) ServeHTTP2(rw http.ResponseWriter, r *http.Request) {
	//pretty.Println(r.Header.Get("Range"), "-", r.Header.Get("User-Agent"))
	//rw.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	//rw.Header().Set("Expires", "0")
	//rw.Header().Set("Pragma", "no-cache")
	//http.ServeFile(rw, r, "/tmp/volume/102/4/100424.mp3")
	// 	streamId := getFormInt("sid", rw, r)
	// 	ts := getFormInt("ts", rw, r)

	// 	t := time.Unix(int64(ts), 0)
	// 	req := manager.RecordRequest{StreamId: uint32(streamId), From: t, To: t.Add(84600 * time.Second)}
	// 	reply := new(manager.RecordReply)

	// 	err := s.rpc.Call("Tracker.GetRecord", req, reply)
	// 	if err != nil {
	// 		http.Error(rw, err.Error(), 500)
	// 	}

	// 	files := make([]SizeReaderAt, 0)
	// 	for _, record := range reply.Record {
	// 		files = append(files, RecordFile(&record))
	// 		glog.V(2).Infof("rec_id: %d %s ts: %d size: %d\n", record.Id, record.Path, record.Time, record.Size)
	// 	}

	// 	sra := NewMultiReaderAt(files...)
	// 	rs := io.NewSectionReader(sra, 0, sra.Size())

	// 	rw.Header().Set("Content-Type", "audio/mpeg")
	// 	http.ServeContent(rw, r, "", t, rs)
}

func (s *streamer) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	pretty.Println(r.Header)

	rw.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	rw.Header().Set("Expires", "0")
	rw.Header().Set("Pragma", "no-cache")

	streamId := getFormInt("sid", rw, r)
	ts := getFormInt("ts", rw, r)

	t := time.Unix(int64(ts), 0)
	req := manager.RecordRequest{StreamId: uint32(streamId), From: t, To: t.Add(84600 * time.Second)}
	reply := new(manager.RecordReply)

	err := s.rpc.Call("Tracker.GetRecord", req, reply)
	if err != nil {
		http.Error(rw, err.Error(), 500)
	}

	parts := make([]SizeReaderAt, 0)
	for _, record := range reply.Record {
		f, err := os.Open(record.Path)
		if err != nil {
			panic(err)
		}
		parts = append(parts, io.NewSectionReader(f, 0, int64(record.Size)))
		//glog.V(2).Infof("rec_id: %d %s ts: %d size: %d\n", record.Id, record.Path, record.Time, record.Size)
		glog.Infoln(record.Path)
	}

	sra := NewMultiReaderAt(parts...)
	rs := io.NewSectionReader(sra, 0, sra.Size())

	rw.Header().Set("Content-Type", "audio/mpeg")
	http.ServeContent(rw, r, "", t, rs)
}

func main() {
	tracker := flag.String("tracker", "localhost:4242", "rpc-tracker address")
	flag.Parse()
	defer glog.Flush()

	rpc := rpc2.NewClient(*tracker)
	rpc.Dial()

	http.Handle("/s", Streamer(rpc))
	http.HandleFunc("/test", handler)
	glog.Fatal(http.ListenAndServe(":8080", nil))
}
