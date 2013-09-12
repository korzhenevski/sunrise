package main

import (
//	"log"
	"sync"
	"time"
)

func main() {
	var wg sync.WaitGroup
	for i := 0; i < 1000000; i++ {
		wg.Add(1)
		go func(i int){
			defer wg.Done()
			for {
				select {
					case <-time.After(600 * time.Second):
						return
				}	
			}
		}(i)
	}
	wg.Wait()
}
