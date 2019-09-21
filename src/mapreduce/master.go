package mapreduce

import (
  "container/list"
  "log"
)
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
    var reply ShutdownReply;
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
  mapschannel := make(chan int, mr.nMap)
  for i := 0; i < mr.nMap; i++ {
    go func(id int) {
      args := DoJobArgs{
        File:          mr.file,
        Operation:     "Map",
        JobNumber:     id,
        NumOtherPhase: mr.nReduce,
      }
      var reply DoJobReply
      for ok := false; !ok;  {
        worker := <-mr.registerChannel
        ok = call(worker, "Worker.DoJob", args, &reply)
        if ok {
          log.Printf("map job %d finished and return worker %s", id, worker)
          mapschannel <- 1
          mr.registerChannel <- worker
          break
        } else{
          log.Printf("map job %d RPC call error with worker %s job", id, worker)
        }
      }

    }(i)
  }
  for i := 0; i < mr.nMap; i++ {
    log.Printf("map finish %d", i)
    <-mapschannel
  }

  log.Printf("Map is done")
  reduceschannel := make(chan int, mr.nReduce)

  for i := 0; i < mr.nReduce; i++ {
    go func(id int) {
      args := DoJobArgs{
        File:          mr.file,
        Operation:     "Reduce",
        JobNumber:     id,
        NumOtherPhase: mr.nMap,
      }
      var reply DoJobReply
      for ok := false; !ok;  {
        worker := <-mr.registerChannel
        ok = call(worker, "Worker.DoJob", args, &reply)
        if ok {
          log.Printf("reduce job %d finished and return worker %s", id, worker)
          reduceschannel <- 1
          mr.registerChannel <- worker
          break
        } else{
          log.Printf("reduce job %d RPC call error with worker %s job", id, worker)
        }
      }



    }(i)
  }
  for i := 0; i < mr.nReduce; i++ {

    <-reduceschannel
    log.Printf("reduce finish %d", i)
  }
  return mr.KillWorkers()
}

