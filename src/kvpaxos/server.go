package kvpaxos

import (
  "net"
  "strconv"
)
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"

const Debug=0

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
  Opeartion string
  Key       string
  Value     string
  Id        int64
}

type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  // Your definitions here.
  logs map[int64]bool
  kvs  map[string]string
  dup  map[int64]string
  seq  int
}

func (kv *KVPaxos) updateData(op Op) string{

  preV := ""

  if op.Opeartion == "PUT" {
      kv.kvs[op.Key] = op.Value
  } else if op.Opeartion == "PUTHASH" {
    if pre, ok := kv.kvs[op.Key]; ok {
      preV = pre
      kv.kvs[op.Key] = strconv.Itoa(int(hash(preV + op.Value)))
    } else {
      kv.kvs[op.Key] = strconv.Itoa(int(hash(op.Value)))
    }

  }
  kv.seq++
  kv.logs[op.Id] = true
  kv.dup[op.Id] = preV
  kv.px.Done(kv.seq)
  return preV
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()
  op := Op{
    Opeartion: "GET",
    Key:       args.Key,
    Value:     "",
    Id:        args.Id,
  }
  success := kv.logs[args.Id]
  if success {
    reply.Value = kv.kvs[args.Key]
    return nil
  }
  for {
    var seop Op
    if decided, instance := kv.px.Status(kv.seq); decided {
      seop = instance.(Op)
      if seop.Id == op.Id {
        break
      }
    } else {
      kv.px.Start(kv.seq+1, op)
      seop = kv.periodCall(kv.seq + 1)
    }
    kv.updateData(seop)
  }
  if v, ok := kv.kvs[args.Key]; ok {
    reply.Value = v
    reply.Err = OK
  } else{
    reply.Err = ErrNoKey
  }

  return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()
  dup, ok := kv.dup[args.Id]
  if ok {
    reply.PreviousValue = dup
    return nil
  }
  var op Op
  if args.DoHash {
    op = Op{
      Opeartion: "PUTHASH",
      Key:       args.Key,
      Value:     args.Value,
      Id:        args.Id,
    }
  } else {
    op = Op{
      Opeartion: "PUT",
      Key:       args.Key,
      Value:     args.Value,
      Id:        args.Id,
    }
  }
  success := kv.logs[args.Id]
  if success {
    reply.PreviousValue = kv.kvs[args.Key]
    return nil
  }
  for {
    var seop Op
    if ok, v := kv.px.Status(kv.seq); ok {
      seop = v.(Op)
      if seop.Id == op.Id {
       break
      }
    } else {
      kv.px.Start(kv.seq+1, op)
      seop = kv.periodCall(kv.seq + 1)
    }

    dupV := kv.updateData(seop)
    reply.PreviousValue = dupV

  }
  return nil
}

func (kv *KVPaxos) periodCall(seq int) Op {
  to := 10 * time.Millisecond
  for {
    decided, instance := kv.px.Status(seq)
    if decided {
      return instance.(Op)
    }
    time.Sleep(to)
    if to < 10 * time.Second {
      to *= 2
    }
  }
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
  DPrintf("Kill(%d): die\n", kv.me)
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
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
  kv.kvs = make(map[string]string)
  kv.dup = make(map[int64]string)
  kv.logs = make(map[int64]bool)
  kv.seq = 0
  // Your initialization code here.

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
        fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  return kv
}

