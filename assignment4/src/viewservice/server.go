package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ClientInfo struct {
	pingCount int
	lastPingArgs *PingArgs
}

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	Viewnum uint
	LastViewAcked bool
	Primary string
	Backup string
	LiveServers map[string]*ClientInfo
	mux sync.Mutex
}

func PrintClientInfo(clientInfo *ClientInfo) {
	//fmt.Printf("Clientinfo: [%v]", clientInfo.lastPingArgs)
}

func (vs *ViewServer) PrintViewServerState() {
	fmt.Printf("primary: %s\n", vs.Primary)
	fmt.Printf("backup: %s\n", vs.Backup)
	fmt.Printf("liveServers: \n[\n")
	for server, clientInfo := range vs.LiveServers {
		fmt.Printf("%s: ", server)
		PrintClientInfo(clientInfo)
		fmt.Printf(", \n")
	}
	fmt.Printf("]\n")
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	if vs.isdead() {
		return nil
	}

	//fmt.Printf("---\nGot ping from client\nargs.Viewnum: %d\nargs.Me: %s\n---\n", args.Viewnum, args.Me)
	
	// spin lock while waiting to acquire mutex
	vs.mux.Lock()

	_, serverUp := vs.LiveServers[args.Me]

	serverCrashed := serverUp && vs.LiveServers[args.Me].lastPingArgs.Viewnum > args.Viewnum

	// if the server crashed and restarted
	if serverCrashed && args.Me == vs.Primary {
		//fmt.Printf("---\nDetected restarted primary: %s\n", vs.Primary)
		vs.Primary = ""
		if vs.Backup != "" {
			//fmt.Printf("Current backup info: ")
			PrintClientInfo(vs.LiveServers[vs.Backup])
			//fmt.Printf("\n---\n")
		}
		if vs.LastViewAcked {
			vs.updateView()
		}
	}
	if serverCrashed && args.Me == vs.Backup {
		//fmt.Printf("---\nDetected restarted backup: %s\n---\n", vs.Backup)
		vs.Backup = ""
		if vs.Primary != "" {
			//fmt.Printf("Current primary info: ")
			PrintClientInfo(vs.LiveServers[vs.Primary])
			//fmt.Printf("\n---\n")
		}
		if vs.LastViewAcked {
			vs.updateView()
		}
	}

	if serverUp {
		newlyInitializedServer := vs.LiveServers[args.Me].lastPingArgs.Viewnum == 0 && args.Viewnum != 0
		vs.LiveServers[args.Me].lastPingArgs = args
		vs.LiveServers[args.Me].pingCount = DeadPings - 1
		// if this server got initialized, we should try to update the view in case it
		// is a newly initialized backup that can become a primary if necessary
		if newlyInitializedServer && vs.LastViewAcked {
			vs.updateView()
		}
	} else {
		ci := new(ClientInfo)
		ci.lastPingArgs = args
		ci.pingCount = DeadPings - 1
		vs.LiveServers[args.Me] = ci
	}

	if args.Viewnum == vs.Viewnum && vs.Primary == args.Me {
		if !vs.LastViewAcked { 
			//fmt.Printf("---\nGot ack for view %d\n---\n", vs.Viewnum) 
		}
		vs.LastViewAcked = true
	}

	v := new(View)
	v.Viewnum = vs.Viewnum
	v.Primary = vs.Primary
	v.Backup = vs.Backup
	reply.View = *v

	vs.mux.Unlock()

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	v := new(View)
	v.Viewnum = vs.Viewnum
	v.Primary = vs.Primary
	v.Backup = vs.Backup
	reply.View = *v
	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	if vs.isdead() {
		return
	}

	// clear off any servers that we haven't gotten a ping for,
	// and reset ping status of servers that we did get pings for
	for key, clientInfo := range vs.LiveServers {
		if clientInfo.pingCount == 0 {
			//fmt.Printf("---\nServer crashed: %s (PRIMARY: %t), (BACKUP: %t)\n----\n", key, key == vs.Primary, key == vs.Backup)
			delete(vs.LiveServers, key)
		} else {
			vs.LiveServers[key].pingCount = vs.LiveServers[key].pingCount - 1
		}
	}

	if vs.LastViewAcked {
		vs.updateView()
	}
}

func (vs *ViewServer) updateView() {

	//fmt.Printf("---\nupdateView called.\n")
	//vs.PrintViewServerState()
	//fmt.Printf("---\n")

	// update primary and backup based on whether they are still in liveServers
	if vs.Primary != "" {
		_, primaryUp := vs.LiveServers[vs.Primary]
		if !primaryUp {
			vs.Primary = ""
		}
	}
	if vs.Backup != "" {
		_, backupUp := vs.LiveServers[vs.Backup]
		if !backupUp {
			vs.Backup = ""
		}
	}

	viewChanged := false
	viewChangeType := ""

	if vs.Primary == "" && vs.Backup == "" && len(vs.LiveServers) > 0 {
		// if we don't have a primary or a backup, choose one of the liveServers
		// to be the primary
		for key, clientInfo := range vs.LiveServers {
			if vs.Viewnum == 0 || clientInfo.lastPingArgs.Viewnum != 0 {
				vs.Primary = key
				break
			}
		}
		viewChangeType = "no_primary-no_backup => primary-no_backup"
		viewChanged = true
	} else if vs.Primary == "" && vs.Backup != "" && vs.LiveServers[vs.Backup].lastPingArgs.Viewnum != 0 {
		// if we do have a backup but no primary, promote the backup to primary if it is initialized
		vs.Primary = vs.Backup
		vs.Backup = ""
		viewChangeType = "no_primary-backup => primary-no_backup"
		viewChanged = true
	}

	// this should not execute for the no_primary-no_backup => primary-no_backup transition
	if vs.Primary != "" && vs.Backup == "" && len(vs.LiveServers) > 1 && 
		viewChangeType != "no_primary-no_backup => primary-no_backup" {
		// if we do not have a backup, but a primary, choose one of the liveServers to be a backup
		for key, _ := range vs.LiveServers {
			if key != vs.Primary {
				vs.Backup = key
				if viewChanged {
					viewChangeType = viewChangeType + " => primary-backup"
				} else {
					viewChangeType = "primary-no_backup => primary-backup"
				}
				viewChanged = true
				break
			}
		}
	}
		
	if viewChanged {
		vs.Viewnum += 1
		vs.LastViewAcked = false
		//fmt.Printf("---\nIncremented viewNum to %d, %s\n", vs.Viewnum, viewChangeType)
		//vs.PrintViewServerState()
		//fmt.Printf("---\n")
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.Primary = ""
	vs.Backup = ""
	vs.Viewnum = 0
	vs.LastViewAcked = true
	vs.LiveServers = make(map[string]*ClientInfo)

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
