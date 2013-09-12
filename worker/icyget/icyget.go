package main

import (
	"flag"
	"github.com/kr/pretty"
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

	pretty.Println("META:", chunk.Meta)
}
