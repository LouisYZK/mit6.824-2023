package main

import (
	"fmt"
	"time"
)

func worker(ch chan []string) {
	time.Sleep(1000)
	ch <- []string{"laaaa"}
}

func master(ch chan []string) {
	for strs := range ch {
		if strs[0] == "end" {
			fmt.Println("end....")
			return
		}
		for _, item := range strs {
			fmt.Println(item)
			go worker(ch)
		}
	}
}

func testSelect(ch chan []string) {
	for {
		select {
		case strs := <-ch:
			for _, item := range strs {
				fmt.Println(item)
			}
			return

		default:
			fmt.Println("waiting...")
		}
	}
}

func main() {
	ch := make(chan []string)
	go func() {
		ch <- []string{"nihaoa"}
	}()
	// master(ch)
	testSelect(ch)
}
