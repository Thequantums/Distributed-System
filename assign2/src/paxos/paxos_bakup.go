package paxos

// //
// // Paxos library, to be included in an application.
// // Multiple applications will run, each including
// // a Paxos peer.
// //
// // Manages a sequence of agreed-on values.
// // The set of peers is fixed.
// // Copes with network failures (partition, msg loss, &c).
// // Does not store anything persistently, so cannot handle crash+restart.
// //
// // The application interface:
// //
// // px = paxos.Make(peers []string, me string)
// // px.Start(seq int, v interface{}) -- start agreement on new instance
// // px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// // px.Done(seq int) -- ok to forget all instances <= seq
// // px.Max() int -- highest instance seq known, or -1
// // px.Min() int -- instances before this seq have been forgotten
// //

// import "net"
// import "net/rpc"
// import "log"

// import "os"
// import "syscall"
// import "sync"
// import "sync/atomic"
// import "fmt"
// import "math/rand"
// import "time"
// import "math"
// // import "sort"

// // px.Status() return values, indicating
// // whether an agreement has been decided,
// // or Paxos has not yet reached agreement,
// // or it was agreed but forgotten (i.e. < Min()).
// type Fate int

// const (
// 	Decided   Fate = iota + 1
// 	Pending        // not yet decided.
// 	Forgotten      // decided but forgotten.
// )

// type PaxosNum struct {
// 	Num int
// 	Id int
// }

// type AcceptedValue struct {
// 	Num PaxosNum
// 	Value interface {}
// }

// type PaxosNumList []AcceptedValue

// func (a PaxosNumList) Len() int           { return len(a) }
// func (a PaxosNumList) Less(i, j int) bool { return comparePaxosNums(a[i].Num, a[j].Num) < 0 }
// func (a PaxosNumList) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// type PromiseRequestArgs struct {
// 	SeqNum int
// 	PxId int
// 	ProposalNum PaxosNum
// 	HighestDone int
// }

// type PromiseRequestReply struct {
// 	SeqNum int
// 	HighestPromised PaxosNum
// 	HighestAccepted PaxosNum
// 	AcceptedValue interface {}
// 	HighestDone int
// }

// type SuggestRequestArgs struct {
// 	SeqNum int
// 	PxId int
// 	ProposalNum PaxosNum
// 	Value interface {}
// 	HighestDone int
// }

// type SuggestRequestReply struct {
// 	Accepted bool
// }

// type Decision struct {
// 	SeqNum int
// 	Id int
// 	Num PaxosNum
// 	Value interface {}
// 	HighestDone int
// }

// type PaxosInstance struct {
// 	status Fate
// 	highestPromised PaxosNum
// 	highestAccepted PaxosNum
// 	acceptedValue interface {}
// }

// type Paxos struct {
// 	mu         sync.Mutex
// 	l          net.Listener
// 	dead       int32 // for testing
// 	unreliable int32 // for testing
// 	rpcCount   int32 // for testing
// 	peers      []string
// 	me         int // index into peers[]

// 	// Your data here.
// 	instances map[int]*PaxosInstance
// 	instanceMapMutex sync.Mutex
// 	instanceMapLockCount int
// 	instanceMapUnlockCount int
	
// 	dones []int
// 	donesMutex sync.Mutex
// 	donesLockCount int
// 	donesUnlockCount int

// 	highestDone int
// 	highestKnown int
// 	currentMin int
// }

// func (px *Paxos) Log(msg string, args ...interface{}) {
// 	// fmt.Printf("---\n%s: ", px.peers[px.me])
// 	// fmt.Printf(msg, args...)
// 	// fmt.Printf("\n")
// }

// func (px *Paxos) instanceMapLock(location string) {
// 	px.instanceMapMutex.Lock()
// 	px.instanceMapLockCount += 1
// //	px.Log("Entered instance map lock (lock count: %d, location: %s)", px.instanceMapLockCount, location)
// }

// func (px *Paxos) instanceMapUnlock(location string) {
// 	px.instanceMapUnlockCount += 1
// 	px.instanceMapMutex.Unlock()
// //	px.Log("Exited instance map lock (unlock count: %d, location: %s)", px.instanceMapUnlockCount, location)
// }

// func (px *Paxos) donesLock(location string) {
// 	px.donesMutex.Lock()
// 	px.donesLockCount += 1
// //	px.Log("Entered dones lock (lock count: %d, location: %s)", px.donesLockCount, location)
// }

// func (px *Paxos) donesUnlock(location string) {
// 	px.donesUnlockCount += 1
// 	px.donesMutex.Unlock()
// //	px.Log("Exited dones lock (lock count: %d, location: %s)", px.donesUnlockCount, location)
// }

// // returns -1 if num1 < num2, 0 if num1 == num2, 1 if num1 > num2
// func comparePaxosNums(num1 PaxosNum, num2 PaxosNum) int {
// 	if num1.Num > num2.Num {
// 		return 1
// 	} else if num1.Num < num2.Num {
// 		return -1
// 	} else {
// 		if num1.Id > num2.Id {
// 			return 1
// 		} else if num1.Id < num2.Id {
// 			return -1
// 		} else {
// 			return 0
// 		}
// 	}
// }

// //
// // call() sends an RPC to the rpcname handler on server srv
// // with arguments args, waits for the reply, and leaves the
// // reply in reply. the reply argument should be a pointer
// // to a reply structure.
// //
// // the return value is true if the server responded, and false
// // if call() was not able to contact the server. in particular,
// // the replys contents are only valid if call() returned true.
// //
// // you should assume that call() will time out and return an
// // error after a while if it does not get a reply from the server.
// //
// // please use call() to send all RPCs, in client.go and server.go.
// // please do not change this function.
// //
// func call(srv string, name string, args interface{}, reply interface{}) bool {
// 	c, err := rpc.Dial("unix", srv)
// 	if err != nil {
// 		err1 := err.(*net.OpError)
// 		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
// 			fmt.Printf("paxos Dial() failed: %v\n", err1)
// 		}
// 		return false
// 	}
// 	defer c.Close()

// 	err = c.Call(name, args, reply)
// 	if err == nil {
// 		return true
// 	}

// 	fmt.Println(err)
// 	return false
// }

// func (px *Paxos) handlePromiseReply(reply *PromiseRequestReply, pxId int, promiseReplies map[PaxosNum]int, highestPreviouslyAccepted *PaxosNum, highestPreviouslyAcceptedValue *interface{}, serverAddress string, acceptedNums *[]AcceptedValue) {

// 	px.Log("Got promise response from %s, %v", serverAddress, reply)
	
// 	// record the reply's accepted number
// 	*acceptedNums = append(*acceptedNums, AcceptedValue{reply.HighestAccepted, reply.AcceptedValue})

// 	// if we get a response from the server, increment the count for that promised number
// 	promisedCount, found := promiseReplies[reply.HighestPromised]
// 	promisedCountPrev := 0
// 	if found {
// 		promisedCountPrev = promisedCount
// 	}
// 	promiseReplies[reply.HighestPromised] = promisedCountPrev + 1
	
// 	// if the server previously accepted a value, record it
// 	if comparePaxosNums(reply.HighestAccepted, *highestPreviouslyAccepted) == 1 {
// 		*highestPreviouslyAccepted = reply.HighestAccepted
// 		*highestPreviouslyAcceptedValue = reply.AcceptedValue
// 	}
	
// 	// update peer's highest done value, and check for new min
// 	updated := px.updateDones(pxId, reply.HighestDone)
// 	if updated {
// 		px.checkOtherDones()
// 	}	
// }

// func (px *Paxos) proposerRoutine(seq int, v interface{}) {

// 	px.instanceMapLock("proposer routine initialization")

// 	// handle the case where another paxos server already called Start on the
// 	// same seq number; in this case just don't do anything to avoid contention
// 	if _, ok := px.instances[seq]; ok {
// 		px.Log("In proposerRoutine initialization, found a paxos instance for seq %d already, exiting proposerRoutine.", seq)
// 		px.instanceMapUnlock("proposer routine initialization")
// 		return
// 	} else {
// 		px.Log("In proposerRoutine initialization, did not find previous paxos instance for seq %d.", seq)
// 	}

// 	// if there's no paxos instance yet for this sequence number, create one
// 	px.instances[seq] = &PaxosInstance{Pending, PaxosNum{-1, -1}, PaxosNum{-1, -1}, nil}

// 	px.Log("Created new paxos instance: %v", px.instances[seq])
	
// 	px.instanceMapUnlock("proposer routine initialization")

	
// 	currentNum := PaxosNum{0, px.me}
// 	currentWaitMultiplier := 2
// 	for true {

// 		if px.isdead() {
// 			break
// 		}

// 		px.instanceMapLock("proposer routine paxos loop init")

// 		// assume that there is a paxos instance if we are in this loop
// 		pxInstance, ok := px.instances[seq]

// 		if !ok {
// 			px.Log("In proposerRoutine, paxos instance for seq %d was deleted in middle of proposerRoutine, exiting...\n", seq)
// 			return
// 		}

// 		// if there is a paxos instance and it's already been decided, don't iterate anymore
// 		if pxInstance.status == Decided {
// 			px.Log("In proposerRoutine paxos loop initialization, found that seq %d was already decided.", seq)
// 			px.instanceMapUnlock("proposer routine paxos loop init")
// 			break
// 		}

// 		px.instanceMapUnlock("proposer routine paxos loop init")

// 		// make promise request to send to servers
// 		promiseArgs := new(PromiseRequestArgs)
// 		promiseArgs.SeqNum = seq
// 		promiseArgs.PxId = px.me
// 		promiseArgs.ProposalNum = currentNum
// 		promiseArgs.HighestDone = px.highestDone
// 		promiseReplies := make(map[PaxosNum]int)
// 		highestPreviouslyAccepted := PaxosNum{-1, -1}
// 		var highestPreviouslyAcceptedValue interface{} = nil
// 		acceptedNums := make([]AcceptedValue, 0)
// 		// loop through all servers and send promise requests to all of them, one at a time
// 		for pxId, serverAddress := range px.peers {
// 			reply := new(PromiseRequestReply) 

// 			px.Log("Sending promise request for seq %d to %s, num %d", seq, serverAddress, currentNum)

// 			// if we're sending the promise request to ourself, do it through function call instead
// 			// of rpc
// 			if pxId == px.me {
// 				px.PromiseRequest(promiseArgs, reply)
// 				px.handlePromiseReply(reply, pxId, promiseReplies, &highestPreviouslyAccepted, &highestPreviouslyAcceptedValue, serverAddress, &acceptedNums)
// 				continue
// 			}

// 			doneChannel := make(chan bool)
// 			go func() {
// 				ok := call(serverAddress, "Paxos.PromiseRequest", promiseArgs, &reply)
// 				if ok {
// 					doneChannel <- true
// 				}
// 			}()
// 			select {
// 			case res := <- doneChannel:
// 				if res {
// 					px.handlePromiseReply(reply, pxId, promiseReplies, &highestPreviouslyAccepted, &highestPreviouslyAcceptedValue, serverAddress, &acceptedNums)
					
// 				}
// 			case <-time.After(50 * time.Millisecond):
// 				px.Log("Did not get promise response from server %s in time.", serverAddress)
// 			}
// 		}

// 		// // check to see if a majority already accepted the same paxos number based on their responses; if they have, then
// 		// // we know this instance is already decided, and the value is the value corresponding to that number
// 		// if len(acceptedNums) > 0 {
// 		// 	sort.Sort(PaxosNumList(acceptedNums))
// 		// 	px.Log("Sorted accepted paxos nums: %v", acceptedNums)
// 		// 	majorityPaxosNum := PaxosNum{-1, -1}
// 		// 	var majorityPaxosValue interface {} = nil
// 		// 	count := 0
// 		// 	lastPaxosNum := acceptedNums[0].Num
// 		// 	for _, acceptedValue := range acceptedNums {
// 		// 		paxosNum := acceptedValue.Num
// 		// 		compareResult := comparePaxosNums(lastPaxosNum, paxosNum)
// 		// 		if compareResult == 0 {
// 		// 			count += 1
// 		// 		} else {
// 		// 			lastPaxosNum = paxosNum
// 		// 			count = 1
// 		// 		}
// 		// 		if count > len(px.peers)/2 && comparePaxosNums(PaxosNum{-1, -1}, paxosNum) != 0 {
// 		// 			majorityPaxosNum = paxosNum
// 		// 			majorityPaxosValue = acceptedValue.Value
// 		// 			break
// 		// 		}
// 		// 	}
// 		// 	if comparePaxosNums(PaxosNum{-1, -1}, majorityPaxosNum) != 0 {
// 		// 		px.Log("Found that majority of peers accepted paxos num %v, so seq %d is decided", majorityPaxosNum, seq)
// 		// 		px.instanceMapLock("proposerRoutine, found majority accept from promise responses")
// 		// 		px.instances[seq].status = Decided
// 		// 		px.instances[seq].acceptedValue = majorityPaxosValue
// 		// 		px.instanceMapUnlock("proposerRoutine, found majority accept from promise responses")
// 		// 	}
// 		// }
		
// 		highestPromised := PaxosNum{-1, -1}
// 		highestPromiseCount := 0
// 		for promiseNum, promiseCount := range promiseReplies {
// 			if comparePaxosNums(promiseNum, highestPromised) == 1 {
// 				highestPromised = promiseNum
// 				highestPromiseCount = promiseCount
// 			}
// 		}

// 		px.Log("Highest promised: %v, Highest promised count: %d, Total peers: %d", highestPromised, highestPromiseCount, len(px.peers))

// 		if comparePaxosNums(highestPromised, currentNum) != 0 || highestPromiseCount <= len(px.peers)/2 {
// 			px.Log("Failed to get majority to promise for %v (CURRENTNUM REJECTED %t, CURRENTNUM FAILED MAJORITY %t)", currentNum, highestPromised != currentNum, highestPromiseCount <= len(px.peers)/2)
			
// 			// wait for a random amount of time to avoid further contention
// 			time.Sleep(time.Duration(((1 + rand.Intn(currentWaitMultiplier)) * 500) % 1000) * time.Millisecond)
// 			currentNum.Num = highestPromised.Num + 1

// 			continue

// 		} else {
// 			px.Log("Successfully got majority to promise for %v", highestPromised)
// 		}

// 		// we have gotten promises from majority, so send suggest requests now

// 		// make suggest request to send to servers
// 		suggestArgs := new(SuggestRequestArgs)
// 		suggestArgs.SeqNum = seq
// 		suggestArgs.PxId = px.me
// 		suggestArgs.ProposalNum = currentNum
// 		// if there was a value previously accepted, propose that value instead of our value
// 		if comparePaxosNums(highestPreviouslyAccepted, PaxosNum{-1, -1}) == 1 {
// 			suggestArgs.Value = highestPreviouslyAcceptedValue
// 		} else {
// 			suggestArgs.Value = v
// 		}

// 		numAccepts := 0
// 		// loop through all servers and send suggest requests to all of them, one at a time
// 		for pxId, serverAddress := range px.peers {
// 			px.Log("Sending suggest request to %s, seq %d num %d", serverAddress, seq, currentNum)

// 			reply := SuggestRequestReply{false}

// 			// if we're sending the suggest request to ourself, do it through function call instead
// 			// of rpc
// 			if pxId == px.me {
// 				px.SuggestRequest(suggestArgs, &reply)
// 			} else {
// 				suggestOk := call(serverAddress, "Paxos.SuggestRequest", suggestArgs, &reply)
// 				if !suggestOk {
// 					continue
// 				}
// 			}

// 			if reply.Accepted {
// 				numAccepts += 1
// 			}
// 		}

// 		if numAccepts > len(px.peers)/2 {
// 			px.Log("In proposerRoutine, got majority of peers (%d out of %d) to accept suggestion %v for seq %d",
// 				numAccepts, len(px.peers), currentNum, seq)

// 			decision := &Decision{seq, px.me, suggestArgs.ProposalNum, suggestArgs.Value, px.highestDone}
// 			// tell all other servers including ourself that we are decided
// 			for pxId, serverAddress := range px.peers {
// 				if pxId == px.me {
// 					px.NotifyDecision(decision, nil)
// 					continue
// 				} else {
// 					call(serverAddress, "Paxos.NotifyDecision", decision, nil)
// 				}
// 			}
// 		}
// 	}
// }

// func (px *Paxos) PromiseRequest(args *PromiseRequestArgs, reply *PromiseRequestReply) error {
	
// 	// update highest dones
// 	updated := px.updateDones(args.PxId, args.HighestDone)
// 	if updated {
// 		px.checkOtherDones()
// 	}

// 	px.instanceMapLock("promise request")

// 	//px.Log("Got promise request for seq %d, num %d", args.SeqNum, args.ProposalNum)

// 	px.updateMax(args.SeqNum)

// 	instance, ok := px.instances[args.SeqNum]

// 	// if there wasn't already an instance, create it
// 	if !ok {
// 		px.instances[args.SeqNum] = &PaxosInstance{Pending, PaxosNum{-1, -1}, PaxosNum{-1, -1}, nil}
// 		instance = px.instances[args.SeqNum]
// 	}

// 	if comparePaxosNums(args.ProposalNum, instance.highestPromised) == 1 {
// 		instance.highestPromised = args.ProposalNum
// 	}

// 	reply.SeqNum = args.SeqNum
// 	reply.HighestPromised = instance.highestPromised
// 	reply.HighestAccepted = instance.highestAccepted
// 	reply.AcceptedValue = instance.acceptedValue

// 	px.instanceMapUnlock("promise request")

// 	return nil
// }

// func (px *Paxos) SuggestRequest(args *SuggestRequestArgs, reply *SuggestRequestReply) error {
	
// 	// update highestDone
// 	updated := px.updateDones(args.PxId, args.HighestDone)
// 	if updated {
// 		px.checkOtherDones()
// 	}

// 	//px.Log("SuggestRequest called for seq %d proposal num %v, haven't entered lock yet...", args.SeqNum, args.ProposalNum)

// 	px.updateMax(args.SeqNum)

// 	px.instanceMapLock("suggest request")

// 	//px.Log("Got suggest request for seq %d, num %d, val %v", args.SeqNum, args.ProposalNum, args.Value)

// 	instance, ok := px.instances[args.SeqNum]

// 	// if there wasn't already an instance, create it
// 	if !ok {
// 		px.instances[args.SeqNum] = &PaxosInstance{Pending, PaxosNum{-1, -1}, PaxosNum{-1, -1}, nil}
// 		instance = px.instances[args.SeqNum]
// 	}

// 	if comparePaxosNums(args.ProposalNum, instance.highestPromised) >= 0 {
// 		//px.Log("Got suggest request with paxos num >= previously promised (got %v, prev was %v)", args.ProposalNum, instance.highestPromised);
// 		instance.acceptedValue = args.Value
// 		instance.highestAccepted = args.ProposalNum
// 		px.instanceMapUnlock("suggest request (suggest accepted)")
		
// 		reply.Accepted = true		
// 	} else {
// 		//px.Log("Got suggest request with paxos num < previously promised (got %v, prev was %v)", args.ProposalNum, instance.highestPromised);
// 		reply.Accepted = false
// 		px.instanceMapUnlock("suggest request (suggest rejected)")
// 	}

// 	//px.Log("Reached end of SuggestRequest for seq %d, num %v", args.SeqNum, args.ProposalNum)
// 	return nil
// }

// func (px *Paxos) NotifyDecision(args *Decision, _ *Decision) error {
	
// 	// update highestDone
// 	updated := px.updateDones(args.Id, args.HighestDone)
// 	if updated {
// 		px.checkOtherDones()
// 	}

// 	//px.Log("Got NotifyDecision, args: %v", args)

// 	px.updateInstanceMapWithDecision(args)

// 	return nil
// }

// func (px *Paxos) updateInstanceMapWithDecision(args *Decision) {
// 	px.instanceMapLock("NotifyDecision")
	
// 	// if there is no instance for this sequence number yet, create one
// 	pxInstance, found := px.instances[args.SeqNum]
	
// 	if !found {
// 		px.instances[args.SeqNum] = &PaxosInstance{Pending, PaxosNum{-1, -1}, PaxosNum{-1, -1}, nil}
// 		pxInstance = px.instances[args.SeqNum]
// 	}

// 	if comparePaxosNums(args.Num, pxInstance.highestAccepted) >= 0 {
// 		px.Log("In updateInstanceMapWithDecision, accepted decision %v, highest accepted %v", args, pxInstance.highestAccepted)
// 		pxInstance.status = Decided
// 		pxInstance.highestAccepted = args.Num
// 		pxInstance.acceptedValue = args.Value
// 	} else {
// 		px.Log("In updateInstanceMapWithDecision, rejected decision %v, highest accepted %v", args, pxInstance.highestAccepted)
// 	}

// 	px.instanceMapUnlock("NotifyDecision")
// }

// // Note from Edward: I originally made this function because I misunderstood the spec and thought that Done was a function that the Paxos instance had to itself call periodically to update its done value based on the sequence numbers that it locally thinks are decided; I just commented out this function in case it's useful later
// // func (px *Paxos) checkLocalDone() {
// // 	px.instanceMapLock("checkLocalDone")

// // 	// iterate through instance map, find which instances are decided, and then check to see
// // 	// if highestDone can be updated

// // 	var decidedSeqs []int
// // 	for seq, instance := range px.instances {
// // 		if (instance.status == Decided) {
// // 			decidedSeqs = append(decidedSeqs, seq)
// // 		}
// // 	}
// // 	sort.Ints(decidedSeqs)

// // 	px.Log("In checkLocalDone, decided seqs: %v", decidedSeqs)

// // 	highestConsecutiveDecided := px.highestDone
// // 	for _, val := range decidedSeqs {
// // 		if val <= highestConsecutiveDecided {
// // 			continue
// // 		} else if val == highestConsecutiveDecided + 1 {
// // 			highestConsecutiveDecided += 1
// // 		} else {
// // 			break
// // 		}
// // 	}
	
// // 	px.Log("In checkLocalDone, highest consecutive decided found: %d", highestConsecutiveDecided)
// // 	px.Done(highestConsecutiveDecided)

// // 	px.instanceMapUnlock("checkLocalDone")

// // 	px.donesLock("checkLocalDone")
// // 	px.dones[px.me] = px.highestDone
// // 	px.donesUnlock("checkLocalDone")
// // }

// func (px *Paxos) updateDones(pxId int, highestDone int) bool {
// 	px.donesLock("updateDones")
// 	updated := false
// 	if highestDone > px.dones[pxId] {
// 		px.dones[pxId] = highestDone
// 		updated = true
// 	}
// 	px.donesUnlock("updateDones")
// 	return updated
// }

// func (px *Paxos) checkOtherDones() {
// 	px.donesLock("checkOtherDones")
	
// 	px.Log("In checkOtherDones, done values of peers %v", px.dones)

// 	minDone := math.MaxInt32
// 	for _, done := range px.dones {
// 		if done < minDone {
// 			minDone = done
// 		}
// 	}

// 	px.Log("In checkOtherDones, found minDone shared across all peers, %d", minDone)
// 	px.currentMin = minDone + 1

// 	px.donesUnlock("checkOtherDones")

// 	px.instanceMapLock("checkOtherDones")

// 	// for i := 0; i < px.currentMin; i++ {
// 	// 	delete(px.instances, i)
// 	// }

// 	px.instanceMapUnlock("checkOtherDones")
// }

// //
// // the application wants paxos to start agreement on
// // instance seq, with proposed value v.
// // Start() returns right away; the application will
// // call Status() to find out if/when agreement
// // is reached.
// //
// func (px *Paxos) Start(seq int, v interface{}) {
// 	px.updateMax(seq)
// 	go px.proposerRoutine(seq, v)
// }

// //
// // the application on this machine is done with
// // all instances <= seq.
// //
// // see the comments for Min() for more explanation.
// //
// func (px *Paxos) Done(seq int) {
// 	px.Log("Done called, setting prev highestDone (%d) to new value %d.", px.highestDone, seq)
// 	px.highestDone = seq
// }

// func (px *Paxos) updateMax(seq int) {
// 	if (seq > px.highestKnown) {
// 		px.Log("In updateMax, setting prev highestKnown (%d) to new value %d.", px.highestKnown, seq)
// 		px.highestKnown = seq
// 	}
// }

// //
// // the application wants to know the
// // highest instance sequence known to
// // this peer.
// //
// func (px *Paxos) Max() int {
// 	return px.highestKnown
// }

// //
// // Min() should return one more than the minimum among z_i,
// // where z_i is the highest number ever passed
// // to Done() on peer i. A peers z_i is -1 if it has
// // never called Done().
// //
// // Paxos is required to have forgotten all information
// // about any instances it knows that are < Min().
// // The point is to free up memory in long-running
// // Paxos-based servers.
// //
// // Paxos peers need to exchange their highest Done()
// // arguments in order to implement Min(). These
// // exchanges can be piggybacked on ordinary Paxos
// // agreement protocol messages, so it is OK if one
// // peers Min does not reflect another Peers Done()
// // until after the next instance is agreed to.
// //
// // The fact that Min() is defined as a minimum over
// // *all* Paxos peers means that Min() cannot increase until
// // all peers have been heard from. So if a peer is dead
// // or unreachable, other peers Min()s will not increase
// // even if all reachable peers call Done. The reason for
// // this is that when the unreachable peer comes back to
// // life, it will need to catch up on instances that it
// // missed -- the other peers therefor cannot forget these
// // instances.
// //
// func (px *Paxos) Min() int {
// 	return px.currentMin
// }

// //
// // the application wants to know whether this
// // peer thinks an instance has been decided,
// // and if so what the agreed value is. Status()
// // should just inspect the local peer state;
// // it should not contact other Paxos peers.
// //
// func (px *Paxos) Status(seq int) (Fate, interface{}) {
// 	px.instanceMapLock("status")
// 	pxInstance, found := px.instances[seq]
// 	px.instanceMapUnlock("status")

// 	// if found {
// 	// 	px.Log("Status called; status for %d was %d", seq, pxInstance.status)
// 	// } else {
// 	// 	px.Log("Status called; no paxos instance found for %d", seq)
// 	// }
	
// 	if !found {
// 		return Pending, nil
// 	} else {
// 		return pxInstance.status, pxInstance.acceptedValue
// 	}
// 	return Pending, nil
// }


// //
// // tell the peer to shut itself down.
// // for testing.
// // please do not change these two functions.
// //
// func (px *Paxos) Kill() {
// 	atomic.StoreInt32(&px.dead, 1)
// 	if px.l != nil {
// 		px.l.Close()
// 	}
// }

// //
// // has this peer been asked to shut down?
// //
// func (px *Paxos) isdead() bool {
// 	return atomic.LoadInt32(&px.dead) != 0
// }

// // please do not change these two functions.
// func (px *Paxos) setunreliable(what bool) {
// 	if what {
// 		atomic.StoreInt32(&px.unreliable, 1)
// 	} else {
// 		atomic.StoreInt32(&px.unreliable, 0)
// 	}
// }

// func (px *Paxos) isunreliable() bool {
// 	return atomic.LoadInt32(&px.unreliable) != 0
// }

// //
// // the application wants to create a paxos peer.
// // the ports of all the paxos peers (including this one)
// // are in peers[]. this servers port is peers[me].
// //
// func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {

// 	px := &Paxos{}
// 	px.peers = peers
// 	px.me = me

// 	// Your initialization code here.
// 	px.instances = make(map[int]*PaxosInstance)
// 	px.dones = make([]int, len(px.peers))
// 	px.instanceMapLockCount = 0
// 	px.instanceMapUnlockCount = 0
// 	px.donesLockCount = 0
// 	px.donesUnlockCount = 0
// 	px.highestDone = -1
// 	px.highestKnown = -1
// 	px.currentMin = 0

// 	if rpcs != nil {
// 		// caller will create socket &c
// 		rpcs.Register(px)
// 	} else {
// 		rpcs = rpc.NewServer()
// 		rpcs.Register(px)

// 		// prepare to receive connections from clients.
// 		// change "unix" to "tcp" to use over a network.
// 		os.Remove(peers[me]) // only needed for "unix"
// 		l, e := net.Listen("unix", peers[me])
// 		if e != nil {
// 			log.Fatal("listen error: ", e)
// 		}
// 		px.l = l

// 		// please do not change any of the following code,
// 		// or do anything to subvert it.

// 		// create a thread to accept RPC connections
// 		go func() {
// 			for px.isdead() == false {
// 				conn, err := px.l.Accept()
// 				if err == nil && px.isdead() == false {
// 					if px.isunreliable() && (rand.Int63()%1000) < 100 {
// 						// discard the request.
// 						conn.Close()
// 					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
// 						// process the request but force discard of reply.
// 						c1 := conn.(*net.UnixConn)
// 						f, _ := c1.File()
// 						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
// 						if err != nil {
// 							fmt.Printf("shutdown: %v\n", err)
// 						}
// 						atomic.AddInt32(&px.rpcCount, 1)
// 						go rpcs.ServeConn(conn)
// 					} else {
// 						atomic.AddInt32(&px.rpcCount, 1)
// 						go rpcs.ServeConn(conn)
// 					}
// 				} else if err == nil {
// 					conn.Close()
// 				}
// 				if err != nil && px.isdead() == false {
// 					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
// 				}
// 			}
// 		}()
// 	}


// 	return px
// }
