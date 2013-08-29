package radio

import (
	"bufio"
	"os"
	"path"
)

type Dumper struct {
	dump    *os.File
	buf     *bufio.Writer
	Written uint32
	Path    string
}

func (d *Dumper) Open(dumpPath string) {
	d.Close()

	d.Path = dumpPath
	err := os.MkdirAll(path.Dir(dumpPath), 0777)
	if err != nil {
		panic(err)
	}
	d.dump, err = os.Create(d.Path)
	if err != nil {
		panic(err)
	}
	d.buf = bufio.NewWriterSize(d.dump, 1<<19)
}

func (d *Dumper) Write(b []byte) {
	d.Written += uint32(len(b))
	nn, err := d.buf.Write(b)
	if nn < len(b) {
		panic(err)
	}
}

func (d *Dumper) Close() {
	d.Path = ""
	if d.dump != nil {
		d.Written = 0
		d.buf.Flush()
		d.dump.Close()
		d.dump = nil
	}
}
