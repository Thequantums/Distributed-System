package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"


type req_rec struct {
	Operation string
	Key string
	Value string
}

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	data_base map[string]string //data struct to store key/value pairs
	current_view viewservice.View //view from viewservice
	datasync bool // true if data & backup are sync, false if they're not, needed for tick function
	request_records map[int64]req_rec //record of requests from client
}



func (pb *PBServer) Fwdgettobackup (args *GetArgs, reply *GetReply) error {
		//check if iam the current backup
		if pb.current_view.Backup != pb.me {
			reply.Err = ErrWrongServer
			return nil
		}

		//apply Get operation on backup
		value, ok := pb.data_base[args.Key]
		if ok {
			// reply value to client, set err to ok
			reply.Value = value
			reply.Err = OK
		} else {
			//if no key exists, reply back to err
			reply.Err = ErrNoKey
		}

		return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.current_view.Primary != pb.me {
		//it's not a primary, so reply wrong server
		reply.Err = ErrWrongServer
		return nil
	}

	//propagate get to backup
	if pb.current_view.Backup != "" {
		ok := call(pb.current_view.Backup, "PBServer.Fwdgettobackup", args, &reply)
		if !ok || reply.Err == ErrWrongServer {
				// primary & backup are not sync
				reply.Err = ErrWrongServer
				return nil
		}
	}

	// apply get Operation
	value, ok := pb.data_base[args.Key]
	if ok {
		// reply value to client, set err to ok
		reply.Value = value
		reply.Err = OK
	} else {
		//if no key exists, reply back to err
		reply.Err = ErrNoKey
	}

	return nil
}

func (pb *PBServer) Fwdputappendtobackup(args *PutAppendArgs, reply *PutAppendReply) error {
	if pb.current_view.Backup != pb.me {
		//if iam not the backup
		reply.Err = ErrWrongServer
		return nil
	}

	//filter out duplicate put & append request from clients
	prev_req, oka := pb.request_records[args.Id]
	if oka && prev_req.Key == args.Key && prev_req.Value ==args.Value && prev_req.Operation == args.Op  {
		//duplicate put or append
		reply.Err = OK
		return nil
	}
	//apply operation
	// if it's not a duplicate, apply the operation
	if args.Op == "Append" {
		pb.data_base[args.Key] = pb.data_base[args.Key] + args.Value
		reply.Err = OK
	}
	if args.Op == "Put" {
		pb.data_base[args.Key] = args.Value
		reply.Err = OK
	}
	//update request_records
	pb.request_records[args.Id] = req_rec{Key: args.Key, Value: args.Value, Operation: args.Op }
	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.current_view.Primary != pb.me {
		//it's not a primary, so reply wrong server
		reply.Err = ErrWrongServer
		return nil
	}


	//filter out duplicate put & append request from clients
	prev_req, oka := pb.request_records[args.Id]
	if oka && prev_req.Key == args.Key && prev_req.Value ==args.Value && prev_req.Operation == args.Op  {
		//duplicate put or append
		reply.Err = OK
		return nil
	}

	//propagate get to backup
	if pb.current_view.Backup != "" {
		ok := call(pb.current_view.Backup, "PBServer.Fwdputappendtobackup", args, &reply)
		if !ok || reply.Err == ErrWrongServer {
				// let the client try again.
				reply.Err = ErrWrongServer
				return nil
		}
	}

	// if it's not a duplicate, propagate successfully apply the operation
	if args.Op == "Append" {
		// inital value of map is already a empty string.
		pb.data_base[args.Key] = pb.data_base[args.Key] + args.Value
		reply.Err = OK
	}
	if args.Op == "Put" {
		pb.data_base[args.Key] = args.Value
		reply.Err = OK
	}
	//update request_records
	pb.request_records[args.Id] = req_rec{Key: args.Key, Value: args.Value, Operation: args.Op }
	return nil
}


//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//

func (pb *PBServer) Fwddatatobackup (args *Fwddatatobackupargs, reply *Fwdreply) error {
		//ping to vs to confirm primary & backup
		//this function is intended to be implemented by backup. A RPC
		newview, err := pb.vs.Ping(pb.current_view.Viewnum)
		//check for error
		if err != nil {
			fmt.Errorf("current view (%v) ping to view service fails", pb.current_view.Viewnum)
		}
		if newview.Backup != pb.me {
			reply.Err = ErrWrongServer
			return nil
		}

		pb.request_records = args.Request_records_arg
		pb.data_base = args.Data_arg

		reply.Err = OK
		return nil

}

func (pb *PBServer) tick() {
	pb.mu.Lock()

	defer pb.mu.Unlock() //ensure the lock will be unlocked after func called

	//ping view service for current view
	newview, err := pb.vs.Ping(pb.current_view.Viewnum)
	if err != nil {
		fmt.Errorf("current view (%v) ping to view service fails", pb.current_view.Viewnum)
	}
	//check if view is changed (backup is changed), then set sync to false.
	//so primary will sync with backup
	if newview.Backup != pb.current_view.Backup && newview.Primary == pb.me && newview.Backup != "" {
		pb.datasync = false
	}

	// for every tick if check if the primary and backup is not sync, then sync.
	// datasync is false when put, get, append operation on backup
	if pb.datasync == false {
		//pb.datasync = false
		var reply Fwdreply
		args := Fwddatatobackupargs{Data_arg: pb.data_base, Request_records_arg: pb.request_records}
		ok := call(newview.Backup, "PBServer.Fwddatatobackup", args, &reply)
		pb.datasync = true
		if !ok || reply.Err != OK {
			// Primary and & Backup failed to sync. Sync again.after the next tick()
			pb.datasync = false
		}
	}
	pb.current_view = newview
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.current_view = viewservice.View{Viewnum: 0, Primary: "", Backup: ""}
	pb.datasync = true //assuming the server start in sync
	pb.data_base = make(map[string]string)
	pb.request_records = make(map[int64]req_rec)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
