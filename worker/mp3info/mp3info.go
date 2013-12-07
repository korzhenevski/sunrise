package main

import (
	// "bufio"
	"flag"
	"github.com/outself/sunrise/mp3"
	"log"
	"os"
)

func main() {
	infile := flag.String("f", "", "file")
	flag.Parse()

	if *infile == "" {
		log.Fatal("infile required")
	}

	var err error

	f, err := os.Open(*infile)
	if err != nil {
		log.Fatal(err)
	}

	frame, err := mp3.GetFirstFrame(f)
	if err != nil {
		panic(err)
	}

	log.Print(frame.Bitrate, frame.SampleRate)
}
