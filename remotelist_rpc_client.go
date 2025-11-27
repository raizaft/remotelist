package main

import (
	"fmt"
	"net/rpc"
	"sync"
	"time"
)

type ArgsAppend struct {
	ListID int
	Value  int
}
type ArgsGet struct {
	ListID int
	Index  int
}
type ArgsListID struct {
	ListID int
}
type BoolReply struct {
	OK bool
}

func main() {
	client, err := rpc.Dial("tcp", ":5000")
	if err != nil {
		fmt.Print("dialing:", err)
	}
	defer func() { _ = client.Close() }()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 1; i <= 5; i++ {
			args := ArgsAppend{ListID: 1, Value: i * 10}
			var rep BoolReply
			if err := client.Call("RemoteList.Append", args, &rep); err != nil {
				fmt.Println("Append error:", err)
				return
			}
			fmt.Println("Append done:", args.Value)
			time.Sleep(200 * time.Millisecond)
		}
	}()

	go func() {
		defer wg.Done()
		time.Sleep(600 * time.Millisecond)
		for i := 0; i < 3; i++ {
			var removed int
			if err := client.Call("RemoteList.Remove", ArgsListID{ListID: 1}, &removed); err != nil {
				fmt.Println("Remove error:", err)
			} else {
				fmt.Println("Elemento retirado:", removed)
			}
			time.Sleep(300 * time.Millisecond)
		}
	}()

	wg.Wait()

	var size int
	if err := client.Call("RemoteList.Size", ArgsListID{ListID: 1}, &size); err == nil {
		fmt.Println("Size:", size)
	}
	var val int
	if err := client.Call("RemoteList.Get", ArgsGet{ListID: 1, Index: 0}, &val); err != nil {
    fmt.Println("Get error:", err)
	} else {
    fmt.Println("Get[0]:", val)
	}
	
}
