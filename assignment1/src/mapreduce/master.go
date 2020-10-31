package mapreduce

import "container/list"
import "fmt"

type WorkerInfo struct {
	address string
	// You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	total_workers := mr.nMap

	//create channel
	mapchannel := make(chan int, mr.nMap)
	reducechannel := make(chan int, mr.nReduce)
	//(mr,channel,Map,mr.nMap,mr.nReduce)
	var job JobType
	job = Map
	for i := 0; i < total_workers; i++ {
		go func(jobNumber int) {
				for {
					//declare reply structure
					var reply DoJobReply
					worker1 := <- mr.registerChannel
					// assign the current job to new worker if it fails
					//fill out structure for sending job to worker
					args := DoJobArgs{mr.file, job, jobNumber, mr.nReduce}
					// send RPC message to worker to do their job
					ok := call(worker1, "Worker.DoJob", args, &reply)
					//if call returns back, then the job completes
					if ok == true {
						 mapchannel <- 1
						 mr.registerChannel <- worker1
						 break;
					}
				}
		  }(i)
		}
		//wait for map jobs to finsih
		for i := 0; i < total_workers; i++ {
			<-mapchannel
		}
		// Do reduce job
		total_workers = mr.nReduce
		job = Reduce
		for i := 0; i < total_workers; i++ {
			go func(jobNumber int) {
					for {
						var reply2 DoJobReply //redeclare reply structure
						worker2 := <- mr.registerChannel
						// assign the current job to new worker if it fails
						//fill out structure for sending job to worker
						args2 := DoJobArgs{mr.file, job, jobNumber, mr.nMap}
						// send RPC message to worker to do their job
						ok2 := call(worker2, "Worker.DoJob", args2, &reply2)
						//if call returns back, then the job completes
						if ok2 == true {
							 reducechannel <- 1
							 mr.registerChannel <- worker2
							 break;
						}
					}
			  }(i)
			}
		//wait completion for reduce jobs
		for i := 0; i < total_workers; i++ {
			<- reducechannel
			}
	return mr.KillWorkers()
}
