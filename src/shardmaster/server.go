package shardmaster

import (
  "net"
  "time"
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

type ShardMaster struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  configs []Config // indexed by config num
  seq int
}


type Op struct {
  // Your data here.
  GroupId int64
  Operation string
  Pid int64
  Shard int
  Servers []string
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
  // Your code here.
  sm.mu.Lock()
  defer sm.mu.Unlock()
  op := &Op{
    GroupId:   args.GID,
    Pid:       nrand(),
    Operation: "Join",
    Shard:     0,
    Servers:   args.Servers,
  }
  sm.log(op)
  //fmt.Printf("group length after join gid %d is %d\n", args.GID, len(sm.configs[len(sm.configs) - 1].Groups))
  return nil
}

func (sm *ShardMaster) log(op *Op) {
  seq := sm.seq
  to := 10 * time.Millisecond
  for {
    if decided, instance := sm.px.Status(seq); decided {
      seop := instance.(Op)
      if op.Pid == seop.Pid {
        sm.operate(op)
        break
      } else {
        sm.operate(&seop)
      }
      seq++
      to = 10 * time.Millisecond
    } else {
      sm.px.Start(seq, *op)

      time.Sleep(to)
      if to < 10 * time.Second {
        to *= 2
      }
    }
  }
  sm.px.Done(seq)
  sm.seq = seq + 1
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
  // Your code here.
  sm.mu.Lock()
  defer sm.mu.Unlock()
  op := &Op{
    GroupId:   args.GID,
    Operation: "Leave",
    Pid:       nrand(),
    Shard:     0,
    Servers:   nil,
  }
  sm.log(op)
  //fmt.Printf("group length after leave gid %d is %d\n", args.GID, len(sm.configs[len(sm.configs) - 1].Groups))
  return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
  // Your code here.
  sm.mu.Lock()
  defer sm.mu.Unlock()
  op := &Op{
    Pid:       nrand(),
  }
  sm.log(op)
  sm.operateMove(args.Shard, args.GID)
  return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
  // Your code here.
  sm.mu.Lock()
  defer sm.mu.Unlock()
  op := &Op{
    GroupId:   0,
    Pid:       nrand(),
    Shard:     0,
    Servers:   nil,
  }
  sm.log(op)
  if args.Num < 0 || args.Num > len(sm.configs) - 1 {
    reply.Config = sm.configs[len(sm.configs) - 1]
  } else {
    reply.Config = sm.configs[args.Num]
  }
  return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
  sm.dead = true
  sm.l.Close()
  sm.px.Kill()
}

func (sm *ShardMaster) operateLeave(gid int64) {
  old := sm.configs[len(sm.configs) - 1]
  var newConfig Config
  newConfig.Num = len(sm.configs)
  newConfig.Groups = map[int64][]string{}
  for k, v := range old.Groups {
    newConfig.Groups[k] = v
  }
  if _, exist := newConfig.Groups[gid]; exist {
    delete(newConfig.Groups, gid)
  }
  old_shards := old.Shards
  count := make(map[int64]int)
  new_shards := [len(old_shards)]int64{}
  average_len_shards := NShards / (len(newConfig.Groups))
  if average_len_shards == 0 {
    average_len_shards = 1
  }
  if len(newConfig.Groups) == 1 {
    for k,_ := range old_shards {
      for g, _ := range newConfig.Groups {
        new_shards[k] = g
      }
    }
  } else {
    var new_gids []int64
    for k, _ := range newConfig.Groups{
      new_gids = append(new_gids, k)
    }
    for k, v := range old_shards {
      if v == gid {
        for _, g := range new_gids {
          if count[g] < average_len_shards {
            new_shards[k] = g
            count[g] += 1
            break
          }
        }
      } else {
        new_shards[k] = v
        count[v] += 1
        if count[v] > average_len_shards {
          for k1, _ := range newConfig.Groups {
            if count[k1] < average_len_shards {
              new_shards[k] = k1
              count[k1] += 1
              break
            }
          }
        }
      }
    }
  }
  newConfig.Shards = new_shards
  //fmt.Printf("Leave gid %d average %d --- %v\n", gid, average_len_shards, newConfig.Shards)

  sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) operateJoin(gid int64, servers []string) {
  old := sm.configs[len(sm.configs) - 1]
  var newConfig Config
  newConfig.Num = len(sm.configs)
  newConfig.Groups = map[int64][]string{}
  for k, v := range old.Groups {
    newConfig.Groups[k] = v
  }
  if _, exist := newConfig.Groups[gid]; !exist {
    newConfig.Groups[gid] = servers
  }
  old_shards := old.Shards

  new_shards := [len(old_shards)]int64{}
  average_len_shards := NShards / (len(newConfig.Groups))
  if average_len_shards == 0 {
    average_len_shards = 1
  }
  if len(newConfig.Groups) == 1 {
    for k,_ := range old_shards {
      new_shards[k] = gid
    }
  } else {
    count := make(map[int64]int)
    for k, v := range old_shards {
      new_shards[k] = v
      count[v] += 1
      if count[v] > average_len_shards {
        for k1, _ := range newConfig.Groups {
          if count[k1] < average_len_shards {
            new_shards[k] = k1
            count[k1] += 1
            break
          }
        }
      }
    }

  }
  newConfig.Shards = new_shards
  //fmt.Printf("Join %d --- %v\n", gid, newConfig.Shards)
  sm.configs = append(sm.configs, newConfig)
}
func (sm *ShardMaster) operateMove(shard int, gid int64) {
  old := sm.configs[len(sm.configs) - 1]
  var newConfig Config
  newConfig.Num = len(sm.configs)
  newConfig.Groups = map[int64][]string{}
  for k, v := range old.Groups {
    newConfig.Groups[k] = v
  }
  new_shards := [NShards]int64{}
  new_shards = old.Shards
  new_shards[shard] = gid
  newConfig.Shards = new_shards
  sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) operate(xop *Op) {
  if xop.Operation == "Join"{
    sm.operateJoin(xop.GroupId, xop.Servers)
  } else if xop.Operation == "Leave" {
    sm.operateLeave(xop.GroupId)
  } else if xop.Operation == "Move"{
    sm.operateMove(xop.Shard, xop.GroupId)
  }
}
//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
  gob.Register(Op{})

  sm := new(ShardMaster)
  sm.me = me

  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}

  rpcs := rpc.NewServer()
  rpcs.Register(sm)

  sm.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  sm.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for sm.dead == false {
      conn, err := sm.l.Accept()
      if err == nil && sm.dead == false {
        if sm.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if sm.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && sm.dead == false {
        fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
        sm.Kill()
      }
    }
  }()

  return sm
}
