package main

import (
	"fmt"
	"net/rpc"
)

func main() {
	client, err := rpc.Dial("tcp", ":5000")
	if err != nil {
		fmt.Print("dialing:", err)
	}

	// Synchronous call
	var reply bool
	var reply_i int
	err = client.Call("RemoteList.Append", 10, &reply)
	err = client.Call("RemoteList.Append", 20, &reply)
	err = client.Call("RemoteList.Append", 30, &reply)
	err = client.Call("RemoteList.Append", 40, &reply)
	err = client.Call("RemoteList.Append", 50, &reply)

	err = client.Call("RemoteList.Remove", 0, &reply_i)
	if err != nil {
		fmt.Print("Error:", err)
	} else {
		fmt.Println("Elemento retirado:", reply_i)
	}
	err = client.Call("RemoteList.Remove", 0, &reply_i)
	if err != nil {
		fmt.Print("Error:", err)
	} else {
		fmt.Println("Elemento retirado:", reply_i)
	}
}
