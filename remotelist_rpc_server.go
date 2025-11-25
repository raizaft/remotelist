package main

import (
	"fmt"
	"net"
	"net/rpc"
	"ifpb/remotelist/pkg"
)

func main() {
	list := new(remotelist.RemoteList)
	rpcs := rpc.NewServer()
	rpcs.Register(list)
	l, e := net.Listen("tcp", "[localhost]:5000")
	defer l.Close()
	if e != nil {
		fmt.Println("listen error:", e)
	}
	for {
		conn, err := l.Accept()
		if err == nil {
			go rpcs.ServeConn(conn)
		} else {
			break
		}
	}
}
