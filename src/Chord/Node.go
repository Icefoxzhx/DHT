package Chord
import(
	"crypto/sha1"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/rpc"
	"sync"
	"time"
)
const(
	M=160
	FailTimes=32
)
type Edge struct{
	Ip string
	Id *big.Int
}
type KVPair struct{
	Key,Val string
}
type Data struct{
	Map map[string]string
	lock sync.Mutex
}
type LookupType struct{
	Id *big.Int
	cnt int
}

type Node struct{
	Ip string
	Id *big.Int

	successors [M+1]Edge
	predecessor *Edge
	finger [M+1]Edge

	lock sync.Mutex
	fingerId int
	on bool

	data Data
	datapre Data
}

func (this *Node) Init(port string,_ *int)error{
	this.Ip=GetLocalAddress()+":"+port
	this.Id=hashstring(this.Ip)
	this.data.Map=make(map[string]string)
	this.datapre.Map=make(map[string]string)
	this.on=true
	return nil
}
func (this *Node) ping(Ip string) bool{
	var success bool
	for times:=0;times<3;times++{
		ch:=make(chan bool)
		go func(){
			client,err:=rpc.Dial("tcp",Ip)
			if err==nil{
				_=client.Close()
				ch<-true
			}else{
				ch<-false
			}
		}()
		select{
		case success=<-ch:
			if success {
				return true
			}else{
				continue
			}
		case <-time.After(time.Second/2):
			break
		}
	}
	return false
}
func (this *Node) GetSuccessors(_ int,res *[M+1]Edge)error{
	this.lock.Lock()
	for i:=1;i<=M;i++{
		(*res)[i]=this.successors[i]
	}
	this.lock.Unlock()
	return nil
}
func (this *Node) GetPredecessor(_ int,res *Edge)error{
	if this.predecessor!=nil{
		*res=*this.predecessor
	}else{
		res=nil
	}
	return nil
}

func (this *Node) FindSuccessor(pos *LookupType, res *Edge) error{
	pos.cnt++
	if pos.cnt>= FailTimes{
		return errors.New("lookup failure: not found")
	}
	err:= this.fixSuccessors()
	if err!=nil{
		fmt.Println(err)
		return err
	}
	if this.successors[1].Ip== this.Ip||pos.Id.Cmp(this.Id)==0 {
		*res=Edge{this.Ip,new(big.Int).Set(this.Id)}
	}else if between(this.Id,pos.Id, this.successors[1].Id,true){
		*res=Edge{this.successors[1].Ip,new(big.Int).Set(this.successors[1].Id)}
	}else{
		nxt:= this.closestPrecedingNode(pos.Id)
		client,err:=rpc.Dial("tcp",nxt.Ip)
		if err!=nil{
			fmt.Println("nxt not found,waiting...",nxt)
			time.Sleep(time.Second)
			return this.FindSuccessor(pos,res)
		}
		err=client.Call("Node.FindSuccessor",pos,res)
		if err!=nil{
			_=client.Close()
			return err
		}
		_=client.Close()
	}
	return nil
}
func (this *Node) closestPrecedingNode(Id *big.Int)Edge{
	for i:=M;i>0;i--{
		if this.finger[i].Id!=nil&&this.ping(this.finger[i].Ip)&&between(this.Id,this.finger[i].Id,Id,true){
			return this.finger[i]
		}
	}
	return this.successors[1]
}


func (this *Node) MoveData(newNode *big.Int, res *map[string]string) error{
	i:=1
	for ;this.predecessor==nil&&i<FailTimes;i++{
		time.Sleep(time.Second)
	}
	if i==FailTimes{
		return errors.New("can't find predecessor"+this.Ip)
	}
	this.data.lock.Lock()
	this.datapre.lock.Lock()
	this.datapre.Map=make(map[string]string)
	for k,v:=range this.data.Map{
		kId:=hashstring(k)
		if between(this.predecessor.Id,kId,newNode,true){
			(*res)[k]=v
			this.datapre.Map[k]=v
		}
	}
	for k:=range *res{
		delete(this.data.Map,k)
	}
	this.data.lock.Unlock()
	this.datapre.lock.Unlock()
	return nil
}
func (this *Node) MoveDatapre(op int,res *map[string]string) error{
	if op==0{
		this.datapre.lock.Lock()
		for k,v:=range this.datapre.Map{
			(*res)[k]=v
		}
		this.datapre.lock.Unlock()
	}else{
		this.data.lock.Lock()
		for k,v:=range this.data.Map{
			(*res)[k]=v
		}
		this.data.lock.Unlock()
	}
	return nil
}
func (this *Node) PutDatapre(res *map[string]string,_ *int) error{
	this.datapre.lock.Lock()
	for k,v:=range *res{
		this.datapre.Map[k]=v
	}
	this.datapre.lock.Unlock()
	return nil
}
func (this *Node) PutData(res *map[string]string,_ *int) error{
	this.data.lock.Lock()
	for k,v:=range *res{
		this.data.Map[k]=v
	}
	this.data.lock.Unlock()
	return nil
}

func (this *Node) Put(kv KVPair, succ *bool) error{
	this.data.lock.Lock()
	this.data.Map[kv.Key]=kv.Val
	this.data.lock.Unlock()
	err:=this.fixSuccessors()
	if err!=nil{
		return err
	}
	client,err:=rpc.Dial("tcp",this.successors[1].Ip)
	if err!=nil{
		return err
	}
	err=client.Call("Node.Putpre",kv,succ)
	_=client.Close()
	if err!=nil{
		return err
	}
	*succ=true
	return nil
}
func (this *Node) Putpre(kv KVPair,succ *bool) error{
	this.datapre.lock.Lock()
	this.datapre.Map[kv.Key]=kv.Val
	this.datapre.lock.Unlock()
	*succ=true
	return nil
}
func (this *Node) Get(key,val *string)error{
	this.data.lock.Lock()
	str,ok:=this.data.Map[*key]
	this.data.lock.Unlock()
	*val=str
	if ok==false{
		return errors.New("get failed")
	}
	return nil
}
func (this *Node) Delete(key *string,succ *bool)error{
	this.data.lock.Lock()
	_,ok:=this.data.Map[*key]
	if ok==true{
		delete(this.data.Map,*key)
		*succ=true
	}else{
		*succ=false
	}
	this.data.lock.Unlock()
	if *succ==false{
		return nil
	}

	err:=this.fixSuccessors()
	if err!=nil{
		return err
	}
	client,err:=rpc.Dial("tcp",this.successors[1].Ip)
	if err!=nil{
		return err
	}
	err=client.Call("Node.Deletepre",key,succ)
	_=client.Close()
	if err!=nil{
		return err
	}
	return nil
}
func (this *Node) Deletepre(key *string,succ *bool)error{
	this.datapre.lock.Lock()
	delete(this.datapre.Map,*key)
	*succ=true
	this.datapre.lock.Unlock()
	return nil
}

func (this *Node) fixSuccessors() error{
	if this.successors[1].Ip==this.Ip{
		return nil
	}
	this.lock.Lock()

	var p int
	for p=1;p<=M;p++{
		if this.ping(this.successors[p].Ip){
			break
		}
	}

	if p==M+1{
		this.lock.Unlock()
		return errors.New("no valId successor")
	}
	if p==1{
		this.lock.Unlock()
		return nil
	}
	this.successors[1]=this.successors[p]
	if this.successors[p].Ip==this.Ip{
		this.lock.Unlock()
		return nil
	}
	client,err:=rpc.Dial("tcp",this.successors[p].Ip)
	if err!=nil{
		this.lock.Unlock()
		return err
	}

	var list [M+1]Edge
	err=client.Call("Node.GetSuccessors",0,&list)
	_=client.Close()
	if err!=nil{
		this.lock.Unlock()
		return err
	}
	for i:=2;i<=M;i++{
		this.successors[i]=list[i-1]
	}
	this.lock.Unlock()
	return nil
}
//check this->successor->predecessor==this?
func (this *Node) stabilize(){
	for this.on==true{

		//fmt.Println(this.Ip,"stabilizing...")

		err:=this.fixSuccessors()
		if err!=nil {
			fmt.Println(err)
			time.Sleep(time.Second/10)
			continue
		}
		client,err:=rpc.Dial("tcp",this.successors[1].Ip)
		if err!=nil {
			fmt.Println(err)
			time.Sleep(time.Second/10)
			continue
		}
		var pred Edge
		err=client.Call("Node.GetPredecessor",0,&pred)

		if err==nil&&this.ping(pred.Ip){
			if between(this.Id,pred.Id,this.successors[1].Id,false){
				this.lock.Lock()
				this.successors[1]=pred
				this.lock.Unlock()
			}
			_=client.Close()
			client,err=rpc.Dial("tcp",this.successors[1].Ip)
			if err!=nil{
				fmt.Print(err)
				time.Sleep(time.Second/10)
				continue
			}
		}
		err=client.Call("Node.Notify",&Edge{this.Ip,this.Id},nil)
		if err!=nil{
			fmt.Print(err)
			_=client.Close()
			time.Sleep(time.Second/10)
			continue
		}
		var list [M+1]Edge
		err=client.Call("Node.GetSuccessors",0,&list)
		if err!=nil{
			fmt.Print(err)
			_=client.Close()
			time.Sleep(time.Second/10)
			continue
		}
		this.lock.Lock()
		for i:=2;i<=M;i++{
			this.successors[i]=list[i-1]
		}
		this.lock.Unlock()
		_=client.Close()
		time.Sleep(time.Second/10)
	}
}
//notify this of its predecessor pred
func (this *Node) Notify(pred *Edge,_ *int) error{
	if this.predecessor==nil||between(this.predecessor.Id,pred.Id,this.Id,false){
		this.predecessor=pred
		client,err:=rpc.Dial("tcp",this.predecessor.Ip)
		if err!=nil{
			return err
		}
		this.datapre.lock.Lock()
		this.datapre.Map=make(map[string]string)
		err=client.Call("Node.MoveDatapre",1,&this.datapre.Map)
		this.datapre.lock.Unlock()
		_=client.Close()
	}
	return nil
}
func (this *Node) fixfingers(){
	this.fingerId=1
	for this.on==true{
		if this.successors[1].Ip!=this.finger[1].Ip{
			this.fingerId=1
		}

		var res LookupType
		for i:=0;i<5;i++{
			res=LookupType{jump(this.Id,this.fingerId),0}
			err:=this.FindSuccessor(&res,&this.finger[this.fingerId])
			if err==nil{
				break
			}else if i==4{
				fmt.Println("Fixfingers Failed")
				return
			}
			fmt.Println("Fixfingers waiting...")
			time.Sleep(time.Second/10)
		}

		dest:=this.finger[this.fingerId]
		this.fingerId++
		if this.fingerId>M{
			this.fingerId=1
			continue
		}
		for{
			if between(this.Id,jump(this.Id,this.fingerId),dest.Id,true){
				this.finger[this.fingerId]=Edge{dest.Ip,new(big.Int).Set(dest.Id)}
				this.fingerId++
				if this.fingerId>M{
					this.fingerId=1
					break
				}
			}else{
				break
			}
		}
		time.Sleep(time.Second/10)
	}
}
//check if predecessor has failed(quit)
func (this *Node) checkPredecessor(){
	for this.on==true{
		if this.predecessor==nil{
			time.Sleep(time.Second/10)
			continue
		}
		if !this.ping(this.predecessor.Ip){
			this.predecessor=nil
			this.datapre.lock.Lock()
			this.data.lock.Lock()
			for k,v:=range this.datapre.Map{
				this.data.Map[k]=v
			}

			_=this.fixSuccessors()
			if this.successors[1].Ip!=this.Ip&&this.ping(this.successors[1].Ip){
				client,err:=rpc.Dial("tcp",this.successors[1].Ip)
				if err==nil{
					_=client.Call("Node.PutDatapre",&this.datapre.Map,nil)
					_=client.Close()
				}
			}
			this.datapre.Map=make(map[string]string)
			this.datapre.lock.Unlock()
			this.data.lock.Unlock()
		}
		time.Sleep(time.Second/10)
	}
}

func hashstring(s string) *big.Int{
	hash:=sha1.New()
	hash.Write([]byte(s))
	return new(big.Int).SetBytes(hash.Sum(nil))
}
func GetLocalAddress()string{
	var localaddr string
	ifaces,err:=net.Interfaces()
	if err!=nil{
		panic("init:failed to find network interfaces")
	}

	//find the first non-loopback interface with an Ip address
	for _,elt:=range ifaces{
		if elt.Flags&net.FlagLoopback==0&&elt.Flags&net.FlagUp!=0{
			addrs,err:=elt.Addrs()
			if err!=nil{
				panic("init: failed to get addresses for network interface")
			}

			for _,addr:=range addrs{
				Ipnet,ok:=addr.(*net.IPNet)
				if ok{
					if Ip4:=Ipnet.IP.To4();len(Ip4)==net.IPv4len{
						localaddr=Ip4.String()
						break
					}
				}
			}
		}
	}
	if localaddr==""{
		panic("init: failed to find non-loopback interface with valId address on this node")
	}
	return localaddr
}
func between(x,y,z *big.Int, inclusive bool) bool{
	if z.Cmp(x)>0{
		return (x.Cmp(y)<0&&y.Cmp(z)<0)||(inclusive&&y.Cmp(z)==0)
	}else{
		return y.Cmp(x)>0||y.Cmp(z)<0||(inclusive&&y.Cmp(z)==0)
	}
}
var(
	two=big.NewInt(2)
	hashMod=new(big.Int).Exp(two,big.NewInt(M),nil)
)
func jump(n *big.Int, pow int) *big.Int{
	gap:=new(big.Int).Exp(two,big.NewInt(int64(pow)-1),nil)
	res:=new(big.Int).Add(n,gap)
	return new(big.Int).Mod(res,hashMod)
}