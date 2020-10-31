package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key string
	Value string
	Operation string
	//piggyback this infor for deleting the request_record once the operation has been servered
	Previous_pid int64
	Current_pid int64
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	data_base map[string]string
	request_record map[int64]string //record of the operation that has been successfully agreed
	current_seq int
}

//This function is to delete the request history once the operation has been servered
//It piggybacking on the Previous_pid to tell what is the previous operation
func (kv *KVPaxos) Freeup_request_record(previous_pid int64){
		if previous_pid != -1 {
			//if the previous id is still there, go ahead and delete it
				_, ok := kv.request_record[previous_pid]
				if ok {
					//delete the previous value from record
						delete(kv.request_record, previous_pid)
				}
			}
}


func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//filter out duplicate Get request
	_, ok := kv.request_record[args.Current_pid]
	if ok {
		reply.Err = OK
		reply.Value = kv.data_base[args.Key]
		return nil
	}

	// prepare value for paxos
	getargs := Op{Key: args.Key, Current_pid: args.Current_pid, Value: "", Operation: "Get", Previous_pid: args.Previous_pid}
	for {
				//kv.current_seq = kv.curren_seq + 1
				status, value := kv.px.Status(kv.current_seq)
				var op_value Op
				//if the status is decided, then retrieve value
				if status == paxos.Decided {
						op_value = value.(Op)
				} else {
					// not yet decided on this sequence number
					kv.px.Start(kv.current_seq, getargs)
					//code from spec
					//waiting for paxos to decide
					to := 10 * time.Millisecond
					for {
							status, value = kv.px.Status(kv.current_seq)
							if status == paxos.Decided {
									op_value = value.(Op)
									break
							}
							time.Sleep(to)
							if to < 10 * time.Second {
								to *= 2
							}
					 }
				}

				// Since we get the consensus of an operation, we store in record history
				//even if it's not the Get operation
				kv.Store(op_value)
				kv.Doputappend(op_value)
				kv.px.Done(kv.current_seq) // done with the current sequence number
				kv.Freeup_request_record(args.Previous_pid)
				kv.current_seq++
				//if the agreed operation is the operation we proposed to paxos, then Done. Else try a bigger seq number
				if op_value.Current_pid == args.Current_pid {
					break
				}
	}

	val, _ := kv.request_record[args.Current_pid]
	if val == ErrNoKey {
			//preparing reply argument
			reply.Value = ""
			reply.Err = ErrNoKey
	}else{
			reply.Value = val
			reply.Err = OK
	}

	return nil
}

func (kv *KVPaxos) Doputappend(args Op) {
	if args.Operation == "Append" {
		  val, ok := kv.data_base[args.Key]
			if ok {
					kv.data_base[args.Key] = val + args.Value
			}else{
					kv.data_base[args.Key] = args.Value
			}
	} else if args.Operation == "Put" {
		kv.data_base[args.Key] = args.Value
	}
}

//store Operation ID in to request_record
func (kv *KVPaxos) Store(args Op) {
		if args.Operation == "Get" {
			v, ok := kv.data_base[args.Key]
			if ok {
				kv.request_record[args.Current_pid]= v
			}else {
				kv.request_record[args.Current_pid] = ErrNoKey
			}
		} else {
				kv.request_record[args.Current_pid] = "Exist"
			}
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, ok := kv.request_record[args.Current_pid]
	if ok {
		//we have seen this request before
		reply.Err = OK
		return nil
	}
	//if not duplicate

	//prepare args for paxos
	putappendargs := Op{Key: args.Key, Value: args.Value, Operation: args.Op, Current_pid: args.Current_pid, Previous_pid: args.Previous_pid}
	//run paxos and wait for agreement and correct sequence number
	for {
			var op_value Op
			status, val := kv.px.Status(kv.current_seq)
			//check if the current sequence is decided
			if status == paxos.Decided {
				//sequence is decided
				op_value = val.(Op)
			} else {
				// not yet decided on this sequence number
				kv.px.Start(kv.current_seq, putappendargs)
				//code from spec
				//waiting for paxos to decide
				to := 10 * time.Millisecond
				for {
						status, val = kv.px.Status(kv.current_seq)
						if status == paxos.Decided {
								op_value = val.(Op)
								break
						}
						time.Sleep(to)
						if to < 10 * time.Second {
							to *= 2
						}
				 }
			 }

			 // Gain consensus on this Operation
			// store operation in request history
			kv.Store(op_value)
			//apply put or append
			kv.Doputappend(op_value)
			//Done with the sequence number
			kv.px.Done(kv.current_seq)
			kv.Freeup_request_record(args.Previous_pid)
			kv.current_seq++
			//If the operation returned by the paxos is the operation we proposed, then Done proposing
			if op_value.Current_pid == args.Current_pid {
					break
				}
	}
	reply.Err = OK

	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.current_seq = 0 //start with current sequence of 0
	//fmt.Printf("hello")
	kv.data_base = make(map[string]string)
	kv.request_record = make(map[int64]string)

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l


	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
