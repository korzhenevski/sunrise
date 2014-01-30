package main

import (
	"bytes"
	"flag"
	"github.com/kr/pretty"
	"github.com/outself/sunrise/mp3"
	"github.com/outself/sunrise/radio"
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

	info := radio.ExtractInfo(r.Header())

	pretty.Println("Name:", info.Name)
	pretty.Println("Content-Type:", info.ContentType)
	pretty.Println("Url:", info.Url)
	pretty.Println("Genre:", info.Genre)
	pretty.Println("Private:", info.Private)
	pretty.Println("Shoutcast:", info.Shoutcast, "\n")

	reader := bytes.NewReader(chunk.Data)
	if frame, err := mp3.GetFirstFrame(reader); err != nil {
		log.Println("mp3_get_first_frame:", err)
	} else {
		pretty.Println("MP3", frame)
	}

	pretty.Println("META:", chunk.Meta)
}
