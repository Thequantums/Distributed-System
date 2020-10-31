package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"
import "math"

const Debug = 0

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type PaxosNum struct {
	Num int
	Id int
}

type AcceptedValue struct {
	Num PaxosNum
	Value interface {}
}

type PaxosNumList []AcceptedValue

func (a PaxosNumList) Len() int           { return len(a) }
func (a PaxosNumList) Less(i, j int) bool { return comparePaxosNums(a[i].Num, a[j].Num) < 0 }
func (a PaxosNumList) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

type PromiseRequestArgs struct {
	SeqNum int
	PxId int
	ProposalNum PaxosNum
	HighestDone int
}

type PromiseRequestReply struct {
	SeqNum int
	HighestPromised PaxosNum
	HighestAccepted PaxosNum
	AcceptedValue interface {}
	HighestDone int
}

type SuggestRequestArgs struct {
	SeqNum int
	PxId int
	ProposalNum PaxosNum
	Value interface {}
	HighestDone int
}

type SuggestRequestReply struct {
	Accepted bool
}

type Decision struct {
	SeqNum int
	Id int
	Num PaxosNum
	Value interface {}
	HighestDone int
}

type PaxosInstance struct {
	status Fate
	highestPromised PaxosNum
	highestAccepted PaxosNum
	acceptedValue interface {}
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	instances map[int]*PaxosInstance
	imm sync.Mutex
	
	dones []int
	dm sync.Mutex

	highestDone int
	highestKnown int
	currentMin int
}

func (px *Paxos) Log(msg string, args ...interface{}) {
	if Debug > 0 {
		printMsg := ""
		printMsg += "---\n"
		printMsg += px.peers[px.me]
		printMsg += ": "
		printMsg += msg
		printMsg += "\n"
		fmt.Printf(printMsg, args...)
	}
}

// returns -1 if num1 < num2, 0 if num1 == num2, 1 if num1 > num2
func comparePaxosNums(num1 PaxosNum, num2 PaxosNum) int {
	if num1.Num > num2.Num {
		return 1
	} else if num1.Num < num2.Num {
		return -1
	} else {
		if num1.Id > num2.Id {
			return 1
		} else if num1.Id < num2.Id {
			return -1
		} else {
			return 0
		}
	}
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}


func (px *Paxos) proposerRoutine(seq int, v interface{}) {

	// create an instance in the paxos instance map for this sequence number
	px.imm.Lock()
	px.instances[seq] = &PaxosInstance{Pending, PaxosNum{-1, -1}, PaxosNum{-1, -1}, nil}
	px.imm.Unlock()

	currentNum := PaxosNum{0, px.me}
	currentVal := v

	for true {

		if px.isdead() {
			break
		}

		px.imm.Lock()
		px.Log("Entering proposerRoutine paxos loop init for seq %d...", seq)
		pxInstance, found := px.instances[seq]
		shouldBreak := false
		if !found {
			px.Log("In proposerRoutine for seq %d, found no instance, breaking...", seq)
			shouldBreak = true
		} else if pxInstance.status == Decided {
			px.Log("In proposerRoutine for seq %d, found was decided, breaking...", seq)
			shouldBreak = true
		}
		px.imm.Unlock()

		if shouldBreak {
			break
		}

		// first, attempt to send promise requests to all servers
		promiseArgs := &PromiseRequestArgs{seq, px.me, currentNum, px.highestDone}
		var pwg sync.WaitGroup
		grantCount := 0
		grantReplies := make(chan *PromiseRequestReply, len(px.peers))
		for i := 0; i < len(px.peers); i++ {
			if i == px.me {
				reply := new(PromiseRequestReply)
				px.PromiseRequest(promiseArgs, reply)
				if comparePaxosNums(promiseArgs.ProposalNum, reply.HighestPromised) == 0 {
					grantCount += 1
					grantReplies <- reply
				}
			} else {
				pwg.Add(1)
				go func(pxId int) {
					defer pwg.Done()
					reply := new(PromiseRequestReply)
					promiseOk := call(px.peers[pxId], "Paxos.PromiseRequest", promiseArgs, &reply)
					if promiseOk && comparePaxosNums(promiseArgs.ProposalNum, reply.HighestPromised) == 0 {
						grantCount += 1
						grantReplies <- reply
					}
				}(i)
			}
		}
		pwg.Wait()
		close(grantReplies)

		if grantCount > len(px.peers)/2 {
			px.Log("Got grants from majority (%d out of %d) peers (seq %d num %v)", grantCount, len(px.peers), seq, currentNum)
		} else {
			px.Log("Failed to get grants from majority of peers, got %d out of %d (seq %d num %v)", grantCount, len(px.peers), seq, currentNum)
			currentNum.Num += 1 // could be made more efficient by checking with learner to see highest accepted num
			continue
		}

		// we have gotten permission from majority to suggest, now make sure to set our value correctly
		highestPreviouslyAcceptedNum := PaxosNum{-1, -1}
		var highestPreviouslyAcceptedValue interface{} = nil
		for reply := range grantReplies {
			if comparePaxosNums(reply.HighestAccepted, highestPreviouslyAcceptedNum) == 1 {
				highestPreviouslyAcceptedNum = reply.HighestAccepted
				highestPreviouslyAcceptedValue = reply.AcceptedValue
			}
		}
		if comparePaxosNums(highestPreviouslyAcceptedNum, PaxosNum{-1, -1}) != 0 {
			px.Log("Found that permission grants included acceptance of %d, val %v", highestPreviouslyAcceptedNum, highestPreviouslyAcceptedValue)
			currentVal = highestPreviouslyAcceptedValue
		}

		px.Log("Suggesting seq %d num %v val %v", seq, currentNum, currentVal)

		suggestArgs := &SuggestRequestArgs{seq, px.me, currentNum, currentVal, px.highestDone}

		// attempt to suggest to all servers
		var swg sync.WaitGroup
		acceptCount := 0
		for i := 0; i < len(px.peers); i++ {
			if i == px.me {
				reply := new(SuggestRequestReply)
				px.SuggestRequest(suggestArgs, reply)
				if reply.Accepted {
					acceptCount += 1
				}
			} else {
				swg.Add(1)
				go func(pxId int) {
					defer swg.Done()
					reply := new(SuggestRequestReply)
					suggestOk := call(px.peers[pxId], "Paxos.SuggestRequest", suggestArgs, &reply)
					if suggestOk && reply.Accepted {
						acceptCount += 1
					}
				}(i)
			}
		}
		swg.Wait()
		
		if acceptCount > len(px.peers)/2 {
			px.Log("Got accepts from majority (%d out of %d) peers (seq %d num %v)", acceptCount, len(px.peers), seq, currentNum)
			decision := &Decision{seq, px.me, currentNum, currentVal, px.highestDone}
			for i := 0; i < len(px.peers); i++ {
				if i == px.me {
					px.NotifyDecision(decision, nil)
				} else {
					call(px.peers[i], "Paxos.NotifyDecision", decision, nil)
				}
			}
		} else {
			px.Log("Failed to get accepts from majority of peers, got %d out of %d (seq %d num %v)", acceptCount, len(px.peers), seq, currentNum)
			currentNum.Num += 1 // could be made more efficient by checking with learner to see highest accepted num
			continue
		}

	}
}

func initNewPaxosInstance() *PaxosInstance {
	i := new(PaxosInstance)
	i.status = Pending
	i.highestPromised = PaxosNum{-1, -1}
	i.highestAccepted = PaxosNum{-1, -1}
	i.acceptedValue = nil
	return i
}

func (px *Paxos) PromiseRequest(args *PromiseRequestArgs, reply *PromiseRequestReply) error {

	px.updateMin(args.HighestDone, args.PxId)

	px.imm.Lock()
	defer px.imm.Unlock()

	px.updateMax(args.SeqNum)

	pxInstance, found := px.instances[args.SeqNum]
	if !found {
		pxInstance = initNewPaxosInstance()
		px.instances[args.SeqNum] = pxInstance
	}
	
	// accept the promise request if it's proposal number is higher than our highest promised
	if comparePaxosNums(args.ProposalNum, pxInstance.highestPromised) == 1 {
		pxInstance.highestPromised = args.ProposalNum
	}

	reply.SeqNum = args.SeqNum
	reply.HighestPromised = pxInstance.highestPromised
	reply.HighestAccepted = pxInstance.highestAccepted
	reply.AcceptedValue = pxInstance.acceptedValue
	reply.HighestDone = px.highestDone

	return nil
}

func (px *Paxos) SuggestRequest(args *SuggestRequestArgs, reply *SuggestRequestReply) error {

	px.updateMin(args.HighestDone, args.PxId)

	px.imm.Lock()
	defer px.imm.Unlock()

	px.updateMax(args.SeqNum)

	pxInstance, found := px.instances[args.SeqNum]
	if !found {
		pxInstance = initNewPaxosInstance()
		px.instances[args.SeqNum] = pxInstance
	}

	if comparePaxosNums(args.ProposalNum, pxInstance.highestPromised) >= 0 {
		px.Log("Accepted suggest request %v", args)
		pxInstance.highestAccepted = args.ProposalNum
		pxInstance.acceptedValue = args.Value
		reply.Accepted = true
	} else {
		reply.Accepted = false
	}

	return nil
}

func (px *Paxos) NotifyDecision(args *Decision, _ *Decision) error {
	
	px.updateMin(args.HighestDone, args.Id)

	px.imm.Lock()
	defer px.imm.Unlock()

	px.updateMax(args.SeqNum)

	px.Log("NotifyDecision with args %v", args)

	pxInstance, found := px.instances[args.SeqNum]
	if !found {
		pxInstance = initNewPaxosInstance()
		px.instances[args.SeqNum] = pxInstance
	}

	pxInstance.status = Decided
	pxInstance.highestPromised = args.Num
	pxInstance.highestAccepted = args.Num
	pxInstance.acceptedValue = args.Value
	
	return nil

}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	px.updateMax(seq)
	go px.proposerRoutine(seq, v)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	if seq > px.highestDone {
		px.Log("Done called, setting prev highestDone (%d) to new value %d.", px.highestDone, seq)
		px.highestDone = seq
		px.updateMin(px.highestDone, px.me)
	}
}

func (px *Paxos) updateMax(seq int) {
	if (seq > px.highestKnown) {
		px.Log("In updateMax, setting prev highestKnown (%d) to new value %d.", px.highestKnown, seq)
		px.highestKnown = seq
	}
}

func (px *Paxos) updateMin(highestDone int, pxId int) {
	px.dm.Lock()
	defer px.dm.Unlock()

	if highestDone > px.dones[pxId] {
		px.dones[pxId] = highestDone

		minDone := math.MaxInt32
		for _, done := range px.dones {
			if done < minDone {
				minDone = done
			}
		}
		px.currentMin = minDone + 1
		px.Log("updateMin update current min to %d, current dones %v", px.currentMin, px.dones)
	}

	px.imm.Lock()

	for seq, _ := range px.instances {
		if seq < px.currentMin {
			px.Log("updateMin deleting memory for seq %d", seq)
			delete(px.instances, seq)
		}
	}

	px.imm.Unlock()
	
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	return px.highestKnown
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	return px.currentMin
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {

	//px.Log("Status called for seq %d", seq)

	if seq < px.currentMin {
		//px.Log("Status for seq %d was %d", seq, Forgotten)
		return Forgotten, nil
	}

	px.imm.Lock()
	defer px.imm.Unlock()

	pxInstance, found := px.instances[seq]

	if found {
		//px.Log("Status for seq %d was %d", seq, pxInstance.status)
		return pxInstance.status, pxInstance.acceptedValue
	} else {
		//px.Log("Status for seq %d was %d", seq, Pending)
		return Pending, nil
	}
}


//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {

	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.instances = make(map[int]*PaxosInstance)
	px.dones = make([]int, len(px.peers))
	for i := range px.dones {
		px.dones[i] = -1
	}
	px.highestDone = -1
	px.highestKnown = -1
	px.currentMin = 0

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}


	return px
}
