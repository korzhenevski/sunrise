package main

import (
	"log"
	// "math/rand"
	// "time"
)

func main() {
	s := "100034"
	log.Print(s[len(s)-1:])
	// rand.Seed(time.Now().UnixNano())
	// log.Println(rand.Float32())
}
