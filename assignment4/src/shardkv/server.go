package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"


const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	Key string
	Value string
	Operation string
	Current_pid int64
	Previous_pid int64
	Configuration_num int //configuration for this operation
}


type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	my_config shardmaster.Config //server's current configuration view
	data_config_records map[int](map[string]string) //used to store data base history for each configuration
	current_seq int //paxos instance number for this server
	data_base map[string]string //storing key value
	request_records map[int64]string //storing history of requests from client
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//getting the shard for the key
	shard := key2shard(args.Key)
	if kv.my_config.Shards[shard] != kv.gid {
		//if the server is not responsible for the key shard, reply wrong group
		reply.Err = ErrWrongGroup
		return nil
	}

	//filter out duplicate get operation
	_, ok := kv.request_records[args.Current_pid]
	if ok {
		reply.Value = kv.data_base[args.Key]
		reply.Err = OK
		return nil
	}

	getargs := Op{Key: args.Key, Value: "", Operation: "Get", Current_pid: args.Current_pid, Previous_pid: args.Previous_pid}
	//apply paxos on the get operation
	for {
			var op_value Op
			kv.current_seq++
			status, val := kv.px.Status(kv.current_seq)
			//check if the current sequence is decided
			if status == paxos.Decided {
				//sequence is decided
				op_value = val.(Op)
			} else {
				// not yet decided on this sequence number
				kv.px.Start(kv.current_seq, getargs)
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
			if op_value.Operation == "Reconfig" {
				kv.DoReconfiguration(op_value.Configuration_num)
			}
			//Done with the sequence number
			kv.px.Done(kv.current_seq)
			kv.Freeup_request_record(args.Previous_pid)

			//If the operation returned by the paxos is the operation we proposed, then Done proposing
			if op_value.Current_pid == args.Current_pid {
					break
				}
	}

	// reply back to client
		val, _ := kv.request_records[args.Current_pid]
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

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//getting shard for the key
	shard := key2shard(args.Key)
	if kv.my_config.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		return nil
	}

	//filter out duplicate put or append operation
	_, ok := kv.request_records[args.Current_pid]
	if ok {
			reply.Err = OK
			return nil
		}

		putappendargs := Op{Key: args.Key, Value: args.Value, Operation: args.Op, Current_pid: args.Current_pid, Previous_pid: args.Previous_pid}
		//apply paxos on the get operation
		for {

				var op_value Op
				kv.current_seq++
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
				if op_value.Operation == "Reconfig" {
					kv.DoReconfiguration(op_value.Configuration_num)
				}
				//Done with the sequence number
				kv.px.Done(kv.current_seq)
				kv.Freeup_request_record(args.Previous_pid)

				//If the operation returned by the paxos is the operation we proposed, then Done proposing
				if op_value.Current_pid == args.Current_pid {
						break
						//fmt.Println("break the putappend operation")
					}
					//fmt.Println("break the putappend operation")
		}

		reply.Err = OK

	return nil
}

//This function is to delete the request history once the operation has been servered
//It piggybacking on the Previous_pid to tell what is the previous operation
func (kv *ShardKV) Freeup_request_record(previous_pid int64){
		if previous_pid != -1 {
			//if the previous id is still there, go ahead and delete it
				_, ok := kv.request_records[previous_pid]
				if ok {
					//delete the previous value from record
						delete(kv.request_records, previous_pid)
				}
			}
}

//store Operation ID in to request_record
func (kv *ShardKV) Store(args Op) {
		if args.Operation == "Get" {
			v, ok := kv.data_base[args.Key]
			if ok {
				kv.request_records[args.Current_pid]= v
			}else {
				kv.request_records[args.Current_pid] = ErrNoKey
			}
		} else if args.Operation == "Put" || args.Operation == "Append" {
				kv.request_records[args.Current_pid] = "Exist"
			}
}

func (kv *ShardKV) Doputappend(args Op) {
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

func (kv *ShardKV) UpdateDatahelper(args *UpdateArgs, reply *UpdateReply) error {
	data_base, ok := kv.data_config_records[args.Configuration_num]
	if ok {
		temp := make(map[string]string)
		for key, val := range data_base {
			temp[key] = val
		}
		reply.Data_base = temp
		reply.Err = OK
	}
	return nil
}

func (kv *ShardKV) DoReconfiguration(latestconfig int) {
	// keep updating data_base with new keys and values until it reaches thte latest config num
	for kv.my_config.Num < latestconfig {
		if kv.my_config.Num == 0 {
			//dont have configuration yet
			kv.my_config = kv.sm.Query(1)
			continue
		}

		save_curr_data_base := make(map[string]string)
		//recheck if the keys in data base should belong to my shards
		for key, val := range kv.data_base {
			shard_each_key := key2shard(key)
			if kv.my_config.Shards[shard_each_key] == kv.gid {
					save_curr_data_base[key] = val
			}
		}
		kv.data_config_records[kv.my_config.Num] = save_curr_data_base

		//getting next configuration
		next_config := kv.sm.Query(kv.my_config.Num + 1)
		for shard, group := range kv.my_config.Shards {
			if next_config.Shards[shard] == kv.gid && group != next_config.Shards[shard] {
					//if the shard i didnt serve become the next shard i will serve
					 //update data base
					 finish := false
					 for !finish {
						 for _, server := range kv.my_config.Groups[group] {
							 //for each server in this group, try to find key value it has for the configuration
							 //then update my data base
							 var reply UpdateReply //used to update database
							 args := UpdateArgs{Configuration_num: kv.my_config.Num}
							 ok := call(server, "ShardKV.UpdateDatahelper", &args, &reply)
							 if ok && reply.Err == OK {
								 //update my data base
								 for key, val := range reply.Data_base {
									 kv.data_base[key] = val
								 }
								 finish = true
								 break
							 }
						 }
					 }
				}
			}
			kv.my_config = next_config
			//fmt.Println("stuck at reconfiguration")
	}
	//fmt.Println("break reconfiguration")
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//Every tick, the server check if its configuration is up to date with the shardmaster
	new_config := (kv.sm.Query(-1)).Num
	if kv.my_config.Num != new_config {
		//run paxos to let every servers to apply update its configuration including me
		reconfig_args := Op{Operation: "Reconfig", Configuration_num: new_config}
		//apply paxos on the get operation
		for {
				var op_value Op
				kv.current_seq++
				status, val := kv.px.Status(kv.current_seq)
				//check if the current sequence is decided
				if status == paxos.Decided {
					//sequence is decided
					op_value = val.(Op)
				} else {
					// not yet decided on this sequence number
					kv.px.Start(kv.current_seq, reconfig_args)
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
				if op_value.Operation == "Reconfig" {
					kv.DoReconfiguration(op_value.Configuration_num)
				}
				//Done with the sequence number
				kv.px.Done(kv.current_seq)
				//kv.Freeup_request_record(args.Previous_pid)

				//If the operation returned by the paxos is the operation we proposed, then Done proposing
				if op_value.Current_pid == reconfig_args.Current_pid {
						break
					}
				//fmt.Println("stuck at tick")
		}
		//fmt.Println("break out of tick")
	}

}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().
	kv.my_config = kv.sm.Query(-1)
	kv.data_base = make(map[string]string)
	kv.request_records = make(map[int64]string)
	kv.current_seq = 0
	kv.data_config_records = make(map[int](map[string]string))

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
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}