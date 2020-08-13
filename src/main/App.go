package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)
func GetLine()[]string{
	reader:=bufio.NewReader(os.Stdin)
	txt,err:=reader.ReadString('\n')
	if err!=nil{
		fmt.Println(err)
		return nil
	}
	return strings.Fields(txt)
}
func Work(){
	nodes:=make([]dhtNode,0)
	nodes=append(nodes,NewNode(10000))
	nodes[0].Run()
	nodes[0].Create()
	Ports:=make(map[int]bool)
	Ports[10000]=true
	Addr:=GetLocalAddress()+":"+strconv.Itoa(10000)
	fmt.Println("Welcome to Icefox's Hash Table")
	fmt.Println("You may enter commands as follows:")
	fmt.Println("help (if you need any help)")
	fmt.Println("add [port] (add a new node with port [port] to the network)")
	fmt.Println("quit (quit network)")
	fmt.Println("put [key] [value] (put a new key-value pair into Icefox's Hash Table)")
	fmt.Println("get [key]  (query for [key] in Icefox's Hash Table)")
	fmt.Println("delete [key]  (delete [key] in Icefox's Hash Table)")
	fmt.Println("ping [Address]  (check if [Address] is still in the network)")
	for flag:=true;flag;{
		args:=GetLine()
		if len(args)==0{
			fmt.Println("Please enter the command like [help].")
			continue
		}
		switch args[0]{
		case "help":
			fmt.Println("You may enter commands as follows:")
			fmt.Println("help (if you need any help)")
			fmt.Println("add [port] (add a new node with port [port] to the network)")
			fmt.Println("quit (quit network)")
			fmt.Println("put [key] [value] (put a new key-value pair into Icefox's Hash Table)")
			fmt.Println("get [key]  (query for [key] in Icefox's Hash Table)")
			fmt.Println("delete [key]  (delete [key] in Icefox's Hash Table)")
			fmt.Println("ping [Address]  (check if [Address] is still in the network)")
		case "add":
			if len(args)!=2{
				fmt.Println("Invalid Command!")
				continue
			}
			port,_:=strconv.Atoi(args[1])
			if port<0||port>65535{
				fmt.Println("Invalid port!")
				continue
			}
			_,exist:=Ports[port]
			if exist{
				fmt.Println("port already exists!")
				continue
			}
			Ports[port]=true
			p:=NewNode(port)
			nodes=append(nodes,p)
			p.Run()
			succ:=p.Join(Addr)
			if !succ{
				fmt.Println("Join failed!")
			}else{
				fmt.Println(time.Now().String()+" Join succeed!")
			}

		case "quit":
			nodes[0].Quit()
			fmt.Println(time.Now().String()+" Quit succeed!")
			flag=false

		case "put":
			if len(args)!=3{
				fmt.Println("Invalid Command!")
				continue
			}
			succ:=nodes[0].Put(args[1],args[2])
			if succ{
				fmt.Println(time.Now().String()+" Put succeed!")
			}else{
				fmt.Println("Put failed!")
			}

		case "get":
			if len(args)!=2{
				fmt.Println("Invalid Command!")
				continue
			}
			succ,val:=nodes[0].Get(args[1])
			if succ{
				fmt.Println(val)
				fmt.Println(time.Now().String()+" Get succeed!")
			}else{
				fmt.Println("Get failed!")
			}

		case "delete":
			if len(args)!=2{
				fmt.Println("Invalid Command!")
				continue
			}
			succ:=nodes[0].Delete(args[1])
			if succ{
				fmt.Println(time.Now().String()+" Delete succeed!")
			}else{
				fmt.Println("Delete failed!")
			}

		case "ping":
			if len(args)!=2{
				fmt.Println("Invalid Command!")
				continue
			}
			succ:=nodes[0].Ping(args[1])
			if succ{
				fmt.Println(time.Now().String()+" Ping succeed!")
			}else{
				fmt.Println("Ping failed!")
			}

		default :
			fmt.Println("Please enter the command like [help].")
		}
	}
}
