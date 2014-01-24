package main

import (
	"flag"
	"github.com/kr/pretty"
	"github.com/outself/sunrise/mp3"
	"github.com/outself/sunrise/radio"

	"bytes"
	"log"
)

func main() {
	url := flag.String("url", "", "stream url")
	ua := flag.String("ua", "WinampMPEG/5.0", "user agent")
	flag.Parse()

	r, err := radio.NewRadio(*url, *ua)
	if err != nil {
		log.Panic(err)
	}
	defer r.Close()
	pretty.Println(r.Header())

	chunk, err := r.ReadChunk()
	if err != nil {
		log.Panic(err)
	}

	reader := bytes.NewReader(chunk.Data)
	if frame, err := mp3.GetFirstFrame(reader); err != nil {
		log.Print(err)
	} else {
		pretty.Println("MP3", frame)
	}

	pretty.Println("META:", chunk.Meta)
}
