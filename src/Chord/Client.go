package Chord

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"
)

type Client struct{
	Node     Node
	Server   *rpc.Server
	wg       *sync.WaitGroup
	Listener net.Listener
}

func (this *Client) Create(){
	this.Node.successors[1].Ip=this.Node.Ip
	this.Node.successors[1].Id=this.Node.Id
	this.Node.predecessor=nil
	go this.Node.stabilize()
	go this.Node.fixfingers()
	go this.Node.checkPredecessor()
}
func (this *Client) Run(){//Print

}
func (this *Client) Join(otherNode string) bool{
	client,err:=rpc.Dial("tcp",otherNode)
	if err!=nil{
		return false
	}
	this.Node.predecessor=nil
	err=client.Call("Node.FindSuccessor",&LookupType{this.Node.Id,0},&this.Node.successors[1])
	if err!=nil{
		_=client.Close()
		fmt.Println(err)
		return this.Join(otherNode)
	}
	_=client.Close()

	client,err=rpc.Dial("tcp",this.Node.successors[1].Ip)
	if err!=nil{
		fmt.Println(err)
		return this.Join(otherNode)
	}
	var list [M+1]Edge
	err=client.Call("Node.GetSuccessors",0,&list)
	if err!=nil{
		_=client.Close()
		fmt.Println(err)
		return this.Join(otherNode)
	}
	this.Node.lock.Lock()
	for i:=2;i<=M;i++{
		this.Node.successors[i]=list[i-1]
	}
	this.Node.lock.Unlock()

	//move data
	this.Node.datapre.lock.Lock()
	err=client.Call("Node.MoveDatapre",0,&this.Node.datapre.Map)
	this.Node.datapre.lock.Unlock()
	if err!=nil{
		_=client.Close()
		fmt.Println("Error: MoveDatapre",err)
		return this.Join(otherNode)
	}

	this.Node.data.lock.Lock()
	err=client.Call("Node.MoveData",this.Node.Id,&this.Node.data.Map)
	this.Node.data.lock.Unlock()
	if err!=nil{
		_=client.Close()
		fmt.Println("Error: MoveData",err)
		return this.Join(otherNode)
	}

	err=client.Call("Node.Notify",&Edge{this.Node.Ip,this.Node.Id},new(int))
	if err!=nil{
		_=client.Close()
		fmt.Println(err)
		return false
	}
	_=client.Close()
	time.Sleep(time.Second/5)
	go this.Node.stabilize()
	go this.Node.fixfingers()
	go this.Node.checkPredecessor()
	return true
}
func (this *Client) Put(key string, val string) bool{
	Id:=hashstring(key)
	var res Edge
	err:=this.Node.FindSuccessor(&LookupType{Id,0},&res)
	if err!=nil{
		fmt.Println("Error:Put ",err)
		return false
	}
	if this.Node.ping(res.Ip)==false{
		fmt.Println("Put Error: not connected")
		return false
	}
	client,err:=rpc.Dial("tcp",res.Ip)
	if err!=nil{
		fmt.Println("Error: Dialing Error: ",err)
		return false
	}
	var succ bool
	err=client.Call("Node.Put",KVPair{key,val},&succ)
	_=client.Close()
	if err!=nil{
		fmt.Println("Error: calling Node.Put:",err)
		return false
	}
	return succ
}
func (this *Client) Get(key string)(succ bool,val string){
	Id:=hashstring(key)
	var dest Edge
	for i:=0;i<5;i++{
		_=this.Node.FindSuccessor(&LookupType{Id,0},&dest)
		client,err:=rpc.Dial("tcp",dest.Ip)
		if err==nil{
			_=client.Call("Node.Get",&key,&val)
			_=client.Close()
		}
		succ= err==nil
		if succ{
			break
		}else{
			time.Sleep(time.Second/3)
		}
	}
	if !succ{
		fmt.Println(dest.Ip," can't find ",key)
	}
	return succ,val
}
func (this *Client) Delete(key string) bool{
	Id:=hashstring(key)
	var dest Edge
	_=this.Node.FindSuccessor(&LookupType{Id,0},&dest)
	client,err:=rpc.Dial("tcp",dest.Ip)
	if err!=nil{
		fmt.Println(err)
		return false
	}
	var succ bool
	err=client.Call("Node.Delete",&key,&succ)
	_=client.Close()
	if err!=nil{
		fmt.Println(err)
		return false
	}
	return succ
}
func (this *Client) Quit(){
	e:=this.Node.fixSuccessors()
	if e!=nil{
		fmt.Println(e)
		return
	}
	if this.Node.successors[1].Ip==this.Node.Ip{
		this.Node.on=false
		_=this.Listener.Close()
		return
	}
	client,err:=rpc.Dial("tcp",this.Node.successors[1].Ip)
	if err!=nil{
		fmt.Println(err)
		return
	}
	this.Node.datapre.lock.Lock()
	err=client.Call("Node.PutDatapre",&this.Node.datapre.Map,nil)
	this.Node.datapre.lock.Unlock()
	this.Node.data.lock.Lock()
	err=client.Call("Node.PutData",&this.Node.data.Map,nil)
	this.Node.data.lock.Unlock()
	_=client.Close()
	this.Node.on=false
	_=this.Listener.Close()
}
func (this *Client) ForceQuit(){
	this.Node.on=false
	_=this.Listener.Close()
	time.Sleep(time.Second)
}
func (this *Client) Ping(addr string)bool{
	return this.Node.ping(addr)
}
func (this *Client) Print(){
	fmt.Println("~~~~~~~~~~~~~~~")
	if this.Node.on==false{
		fmt.Println("Closed.")
		return
	}
	fmt.Println("Ip:",this.Node.Ip,"Id:",this.Node.Id)

	//fmt.Println("data:")
	//for k,v:=range this.Node.data.Map{
	//	fmt.Println(k,":",v)
	//}
	fmt.Println("Predecessor:",this.Node.predecessor)
	fmt.Println("Successor:",this.Node.successors[1])

	fmt.Println(this.Node.data.Map)
	//fmt.Println("finger:")
	//for i,x:=range this.Node.finger{
	//	fmt.Println(i,":",x)
	//}
}