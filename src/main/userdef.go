package main

import (
	"Chord"
	"fmt"
	"net"
	"net/rpc"
	"strconv"
)

func NewNode(port int) dhtNode {
	res:=new(Chord.Client)
	res.Server=rpc.NewServer()
	err:=res.Server.Register(&res.Node)
	if err!=nil{
		fmt.Println(err)
	}
	_=res.Node.Init(strconv.Itoa(port),new(int))
	res.Listener,err=net.Listen("tcp",":"+strconv.Itoa(port))
	if err!=nil{
		fmt.Println("Error: Listen error: ",err)
	}
	go res.Server.Accept(res.Listener)
	return res
}
