package main

import (
	"fmt"
	remotelist "ifpb/remotelist/pkg"
	"net"
	"net/rpc"
	"time"
)

func main() {
	logFile := "operations.log"
	snapFile := "snapshot.json"
	historyMaxLines := 20

	list, err := remotelist.NewRemoteList(logFile, snapFile, 5*time.Second, historyMaxLines)
	if err != nil {
		fmt.Println("failed to init remote list:", err)
		return
	}
	rpcs := rpc.NewServer()
	if err := rpcs.Register(list); err != nil {
		fmt.Println("rpc register:", err)
		return
	}

	l, e := net.Listen("tcp", "localhost:5000")
	if e != nil {
		fmt.Println("listen error:", e)
		return
	}
	defer l.Close()

	fmt.Println("RemoteList RPC server listening on :5000")

	for {
		conn, err := l.Accept()
		if err == nil {
			go rpcs.ServeConn(conn)
		} else {
			fmt.Println("accept error:", err)
			break
		}
	}
}
