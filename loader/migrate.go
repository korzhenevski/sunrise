package main

import (
	"bufio"
	"io"
	"log"
	"os"
	"strings"
)

func AppendFile(infile, outfile string) error {
	in, err := os.Open(infile)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(outfile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0777)
	if err != nil {
		return err
	}
	defer out.Close()

	written, err := io.Copy(out, in)
	if err != nil {
		return err
	}

	if err := os.Remove(infile); err != nil {
		return err
	}

	log.Printf("file out: %s %s %d\n", infile, outfile, written)
	return nil
}

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		row := strings.Split(scanner.Text(), " ")
		if len(row) < 3 {
			continue
		}
		log.Println(row)
		err := AppendFile(row[0], row[1])
		if err != nil {
			log.Print("error", err)
		}
	}
}
