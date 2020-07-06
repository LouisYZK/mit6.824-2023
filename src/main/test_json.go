package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
)

type kv struct {
	Key    string   `json:"key"`
	Values int      `json: "values"`
	list   []string `json: "list"`
}

func main() {
	var data []kv
	for i := 0; i < 10; i++ {
		data = append(data, kv{
			Key:    strconv.Itoa(i),
			Values: i * 4,
			list:   []string{"A", "B"},
		})
	}
	fileName := "test-json"
	file, err := os.Create(fileName)
	if err != nil {
		fmt.Println("create file error")
		return
	}
	enc := json.NewEncoder(file)
	for _, item := range data {
		if err := enc.Encode(item); err != nil {
			fmt.Println("error encoding...", item)
		}
	}
	f, err := os.Open(fileName)
	if err != nil {
		fmt.Println("open file error...")
		return
	}
	dec := json.NewDecoder(f)
	for {
		var kvItem kv
		err := dec.Decode(&kvItem)
		if err != nil {
			break
		}
		fmt.Println(kvItem)
	}
}
