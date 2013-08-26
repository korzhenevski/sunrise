package radio

import (
	"bufio"
	"os"
	"path"
)

type Dumper struct {
	dump *os.File
	buf  *bufio.Writer
	Path string
}

func (d *Dumper) Open(dumpPath string) {
	d.Close()
	d.Path = dumpPath
	err := os.MkdirAll(path.Dir(dumpPath), 0777)
	if err != nil {
		panic(err)
	}

	d.dump, err = os.Create(d.Path)
	d.buf = bufio.NewWriterSize(d.dump, 1<<20)
	if err != nil {
		panic(err)
	}
}

func (d *Dumper) Write(b []byte) (n int, err error) {
	return d.buf.Write(b)
}

func (d *Dumper) Close() {
	d.Path = ""
	if d.dump != nil {
		d.buf.Flush()
		d.dump.Close()
		d.dump = nil
	}
}
