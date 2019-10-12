package pbservice

import (
  "net"
  "strconv"
)
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"

//import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    n, err = fmt.Printf(format, a...)
  }
  return
}

type PBServer struct {
  l net.Listener
  dead bool // for testing
  unreliable bool // for testing
  me string
  vs *viewservice.Clerk
  done sync.WaitGroup
  finish chan interface{}
  // Your declarations here.
  view viewservice.View
  kvs map[string]string
  mu sync.Mutex
  dup map[int64]string
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  pb.mu.Lock()
  //defer pb.mu.Unlock()
  //fmt.Printf("key is %s, value is %s, server is %s\n", args.Key, args.Value, pb.me)
  duplicate, ok := pb.dup[args.Id]
  if !ok {
    if pb.view.Primary == pb.me && !args.Forward {
      if args.DoHash {
        val, hasKey := pb.kvs[args.Key]
        if hasKey {
          reply.PreviousValue = val
          //fmt.Printf("pre value is %s\n", val)
          pb.kvs[args.Key] = strconv.Itoa(int(hash(val + args.Value)))
          pb.dup[args.Id] = val
        } else {
          reply.PreviousValue = ""
          pb.kvs[args.Key] = strconv.Itoa(int(hash(args.Value)))
          pb.dup[args.Id] = ""
        }
      } else {
        pb.kvs[args.Key] = args.Value
      }
      if pb.view.Backup != "" {
        args := &PutArgs{args.Key, args.Value, args.DoHash, true, args.Id}
        var reply PutReply
        ok := call(pb.view.Backup, "PBServer.Put", args, &reply)
        for !ok {
          ok = call(pb.view.Backup, "PBServer.Put", args, &reply)
        }
        pb.mu.Unlock()
        return nil
          //fmt.Printf("PutKvsToBackup error")
      }
    } else if pb.me == pb.view.Backup && args.Forward {
      if args.DoHash {
        val, hasKey := pb.kvs[args.Key]
        if hasKey {
          reply.PreviousValue = val
          pb.kvs[args.Key] = strconv.Itoa(int(hash(val + args.Value)))
          pb.dup[args.Id] = val
        } else {
          reply.PreviousValue = ""
          pb.kvs[args.Key] = strconv.Itoa(int(hash(args.Value)))
          pb.dup[args.Id] = ""
        }
      } else {
        pb.kvs[args.Key] = args.Value
      }
    } else {
      reply.Err = "error"
      pb.mu.Unlock()
      return nil
    }
  } else {
    //fmt.Printf("duplicate id %s\n",duplicate)
    reply.PreviousValue = duplicate
    //fmt.Printf("pre value is %s\n", reply.PreviousValue)
  }
  pb.mu.Unlock()
  return nil
}
func (pb *PBServer) GetFromBackup(args *GetArgs, reply *GetReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()
  if pb.me != pb.view.Backup {
    reply.Err = ErrWrongServer
    return nil
  }
  return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  pb.mu.Lock()
  defer pb.mu.Unlock()
  if pb.me == pb.view.Primary{
    key := args.Key
    reply.Value = pb.kvs[key]
    if pb.view.Backup != "" {
      ok := call(pb.view.Backup, "PBServer.GetFromBackup", args, reply)
      if ok {
        if reply.Err == "error" {

        } else {
          //fmt.Printf("reply value is %s and primary value is %s backup is %s\n", reply.Value, pb.kvs[args.Key], pb.view.Backup)
          return nil
        }
      } else {
        reply.Err = ErrWrongServer
        return nil
      }
    }
  } else {
    reply.Err = "error"
  }
  return nil
}

func (pb *PBServer) GetKvsFromPrimary (args *GetKVsArgs, reply *GetKVsReply) error{
  if pb.view.Primary == pb.me {
    reply.KVs = pb.kvs
    reply.Err = OK
  } else {
    reply.Err = "error"
  }

  return nil
}

//func (pb *PBServer) PutKvsToBackup (args *PutKVsArgs, reply *PutKVsReply) error{
//  if args.DoHash{
//    val, hasKey := pb.kvs[args.Key]
//    if hasKey {
//      pb.kvs[args.Key] = strconv.Itoa(int(hash(val +args.Value)))
//    } else {
//      pb.kvs[args.Key] = args.Value
//    }
//  } else {
//    pb.kvs[args.Key] = args.Value
//  }
//  return nil
//}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
  // Your code here.
  //fmt.Printf("pb is %s", pb.me)
  //pb.mu.Lock()
  //defer pb.mu.Unlock()
  curView,_ := pb.vs.Ping(pb.view.Viewnum)
  //fmt.Printf(curView.Primary)
  //fmt.Printf("ticked: ViewNum: %d, BackUp %s, Primary %s\n", curView.Viewnum, curView.Backup, curView.Primary)
  //fmt.Printf("pbView: ViewNum: %d, BackUp %s, Primary %s\n", pb.view.Viewnum, pb.view.Backup, pb.view.Primary)
  if curView.Viewnum != pb.view.Viewnum {
    //fmt.Printf("curView: ViewNum: %d, BackUp %s, Primary %s\n", curView.Viewnum, curView.Backup, curView.Primary)
    //fmt.Printf("server %s, pbView: ViewNum: %d, BackUp %s, Primary %s\n", pb.me, pb.view.Viewnum, pb.view.Backup, pb.view.Primary)
    pb.view = curView
    //fmt.Printf("after change server %s, pbView: ViewNum: %d, BackUp %s, Primary %s\n", pb.me, pb.view.Viewnum, pb.view.Backup, pb.view.Primary)
    if pb.view.Backup == pb.me {
      args := &GetKVsArgs{}
      var reply GetKVsReply
      ok := call(pb.view.Primary, "PBServer.GetKvsFromPrimary", args, &reply)
      if ok {
        pb.kvs = reply.KVs
      }
    }
  }


}


// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
  pb.dead = true
  pb.l.Close()
}


func StartServer(vshost string, me string) *PBServer {
  pb := new(PBServer)
  pb.me = me
  pb.vs = viewservice.MakeClerk(me, vshost)
  pb.finish = make(chan interface{})
  // Your pb.* initializations here.
  pb.kvs = make(map[string]string)
  pb.dup = make(map[int64]string)
  rpcs := rpc.NewServer()
  rpcs.Register(pb)

  os.Remove(pb.me)
  l, e := net.Listen("unix", pb.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  pb.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for pb.dead == false {
      conn, err := pb.l.Accept()
      if err == nil && pb.dead == false {
        if pb.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        } else {
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && pb.dead == false {
        //fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
        pb.kill()
      }
    }
    DPrintf("%s: wait until all request are done\n", pb.me)
    pb.done.Wait() 
    // If you have an additional thread in your solution, you could
    // have it read to the finish channel to hear when to terminate.
    close(pb.finish)
  }()

  pb.done.Add(1)
  go func() {
    for pb.dead == false {
      //fmt.Printf("server start tick  server %s, pbView: ViewNum: %d, BackUp %s, Primary %s\n", pb.me, pb.view.Viewnum, pb.view.Backup, pb.view.Primary)
      pb.tick()
      time.Sleep(viewservice.PingInterval)
    }
    pb.done.Done()
  }()

  return pb
}
