package shardkv

import (
  "net"
  "strconv"
)
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"

const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
        if Debug > 0 {
                log.Printf(format, a...)
        }
        return
}


type Op struct {
  // Your definitions here.
  Operation string
  Pid int64
  Key string
  Value string
  DoHash bool
  replyValue string
}


type ShardKV struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  sm *shardmaster.Clerk
  px *paxos.Paxos

  gid int64 // my replica group ID

  // Your definitions here.
  seq int
  config shardmaster.Config
  kvs map[string]string
  preValue map[int64]string

}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()
  op := Op{
    Operation: "Get",
    Pid:       args.Id,
    Key:       args.Key,
    Value:     "",
    DoHash:    false,
  }
  kv.log(&op)
  value, exist := kv.preValue[op.Pid]
  if exist {
    reply.Err = OK
    reply.Value = value
  }
  return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()
  op := Op{
    Operation: "Put",
    Pid:       0,
    Key:       args.Key,
    Value:     args.Value,
    DoHash:    args.DoHash,
  }
  //seq := kv.seq
  kv.log(&op)
  value, exist := kv.preValue[op.Pid]
  if exist {
    reply.Err = OK
    reply.PreviousValue = value
  }
  //reply.Err = OK
  //reply.PreviousValue = op.replyValue
  return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
}


// tell the server to shut itself down.
func (kv *ShardKV) kill() {
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

func (kv *ShardKV) log(op *Op) {
  seq := kv.seq
  to := 10 * time.Millisecond
  for {
    if decided, instance := kv.px.Status(seq); decided {
      seop := instance.(Op)
      if op.Pid == seop.Pid {
        if op.Operation == "Put" {
          if op.DoHash {
            kv.preValue[op.Pid] = kv.kvs[op.Key]
            kv.kvs[op.Key] = strconv.Itoa(int(hash(kv.kvs[op.Key] + op.Value)))
            op.replyValue = kv.preValue[op.Pid]
          } else {
            kv.preValue[op.Pid] = kv.kvs[op.Key]
            kv.kvs[op.Key] = op.Value
          }
        } else if op.Operation == "Get"{
          kv.preValue[op.Pid] = kv.kvs[op.Key]
          DPrintf("value is %v\n", kv.preValue[op.Pid])
          op.replyValue = kv.preValue[op.Pid]
        }
        DPrintf("%d",seq)
        break
      } else{

      }
      seq++
      to = 10 * time.Millisecond
    } else {
      kv.px.Start(seq, *op)
      time.Sleep(to)
      if to < 10 * time.Second {
        to *= 2
      }
    }
  }
  kv.px.Done(seq)
  kv.seq = seq + 1
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

  kv.kvs = make(map[string]string)
  kv.preValue = make(map[int64]string)

  // Your initialization code here.
  // Don't call Join().

  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)


  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && kv.dead == false {
        fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  go func() {
    for kv.dead == false {
      kv.tick()
      time.Sleep(250 * time.Millisecond)
    }
  }()

  return kv
}
