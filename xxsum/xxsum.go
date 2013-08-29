package main

import "github.com/vova616/xxhash"
import "io"
import "os"
import "fmt"
import "flag"
import "log"

func main() {
	flag.Parse()
	fname := flag.Arg(0)
	if fname == "" {
		log.Fatal("no file")
	}	

	f, err := os.Open(fname)
	if err != nil {
		panic(err)
	}

	stat, err := os.Stat(fname)
	if err != nil {
		panic(err)
	}

	hasher := xxhash.New(0)
	io.Copy(hasher, f)
	
	fmt.Printf("size: %d, xxhash: %d\n", stat.Size(), hasher.Sum32())
}
