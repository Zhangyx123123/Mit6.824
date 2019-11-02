package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
  "net"
  "time"
)
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"

type InstanceStatus struct {
  V_A    interface{}
  N_P    int
  N_A    int
  Finish bool
}

type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]


  // Your data here.
  instance map[int]*InstanceStatus
  min int
  max int
  dones []int

}


type PrepareArgs struct {
  Seq int
  Timestamp int
}

type PrepareReply struct {
  Value interface{}
  Timestamp int
  Done int
  Error bool
}

type AcceptArgs struct {
  Timestamp int
  Seq int
  Value interface{}
}

type ReplyForAD struct {
  Error bool
  Done int
}

type DecideArgs struct {
  Seq int
  Value interface {}
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
  c, err := rpc.Dial("unix", srv)
  if err != nil {
    err1 := err.(*net.OpError)
    if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
      fmt.Printf("paxos Dial() failed: %v\n", err1)
    }
    return false
  }
  defer c.Close()

  err = c.Call(name, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}


//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
  // Your code here.
  go func() {
    px.Propose(seq, v)
  }()
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  // Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()
  if seq > px.dones[px.me]{
   px.dones[px.me] = seq
  }
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  // Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()
  maxSeq:=-1
  for seq, _ := range px.instance {
   if seq > maxSeq{
     maxSeq = seq
   }
  }
  return maxSeq
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
  // You code here.
  px.mu.Lock()
  defer px.mu.Unlock()
  if px.min < 0 {
    return 0
  }
  for seq, _ := range px.instance {
    if seq < px.min {
      delete(px.instance, seq)
    }
  }
  return px.min
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  // Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()
  _, ok := px.instance[seq]
  if !ok {
    px.instance[seq] = new(InstanceStatus)
  }
  instance := px.instance[seq]
  if !instance.Finish {
    return false, nil
  } else {
    return true, instance.V_A
  }
}


//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}
func (px *Paxos) Prepare(seq int, v interface{}) (int, interface{}, bool) {
  tstamp := (int(time.Now().UnixNano())) + px.me
  px.mu.Lock()
  args := &PrepareArgs{Seq:seq, Timestamp:tstamp}
  px.mu.Unlock()
  count:=0
  replyP := 0
  replyV := v
  for index, peer := range px.peers {
    var reply PrepareReply
    var status bool
    if peer == px.peers[px.me] {
      px.PrepareReceive(args, &reply)
      status = true
    } else{
      status = call(peer, "Paxos.PrepareReceive", args, &reply)
    }
    if status && reply.Error == false {
      count++
      if reply.Timestamp > replyP {
        replyP = reply.Timestamp
        replyV = reply.Value
      }
    }
    px.statusUpdate(reply.Done, index)
  }
  //fmt.Printf("%d\n",count)
  if count > len(px.peers) / 2 {
    //fmt.Printf("a\n")
    return tstamp, replyV, true
  } else {
    //fmt.Printf("b\n")
    return 0, nil, false
  }
}
func (px *Paxos) Propose(seq int, v interface{}) {
  for px.ErrorFree(seq) {
    t, v, ok := px.Prepare(seq, v);
    if ok == false {
      continue
    }
    ok = px.Accept(seq, t, v)
    if ok == false {
      continue
    }
    px.Decide(seq, px.dones[px.me], v)
  }
}

func (px *Paxos) PrepareReceive(args *PrepareArgs, reply *PrepareReply) error{
  px.mu.Lock()
  defer px.mu.Unlock()
  //fmt.Printf("prepare receive start\n")
  _, ok := px.instance[args.Seq]
  if !ok {
    px.instance[args.Seq] = &InstanceStatus{
      V_A: nil,
      N_P: 0,
      N_A: 0,
    }
  }
  instance := px.instance[args.Seq]
  if args.Timestamp > instance.N_P {
    reply.Error = false
    instance.N_P = args.Timestamp
    reply.Timestamp = instance.N_A
    reply.Value = instance.V_A
  } else {
    reply.Error = true
  }

  reply.Done = px.dones[px.me]
  return nil
}
func (px *Paxos) DecideReceive (args *DecideArgs, reply *ReplyForAD) error{
  px.mu.Lock()
  defer px.mu.Unlock()
  //fmt.Printf("decide receive start\n")
  _, ok := px.instance[args.Seq]
  if !ok {
    px.instance[args.Seq] = &InstanceStatus{
      V_A:    nil,
      N_P:    0,
      N_A:    0,
      Finish: false,
    }
  }

  instance := px.instance[args.Seq]
  instance.Finish = true
  instance.V_A = args.Value
  if args.Seq > px.max{
    px.max = args.Seq
  }
  reply.Error = false
  reply.Done = px.dones[px.me]
  return nil
}
func (px *Paxos) Accept(seq int, t int, v interface{}) bool {
  px.mu.Lock()
  args := &AcceptArgs{
    Timestamp: t,
    Seq:       seq,
    Value:     v,
  }
  px.mu.Unlock()
  count := 0
  major := len(px.peers) / 2
  for index, peer := range(px.peers){
    var reply ReplyForAD
    var status bool
    if index == px.me {
      px.AcceptReceive(args, &reply)
      status = true
    } else {
      status = call(peer, "Paxos.AcceptReceive", args, &reply)
    }

    if status && reply.Error == false{
      count+=1
    }
    px.statusUpdate(reply.Done, index)
  }
  //fmt.Printf("%d\n",count)
  if count > major {
    return true
  }
  return false
}

func (px *Paxos) Decide(seq int, done int, v interface{}) error{
  px.mu.Lock()
  args := &DecideArgs{
    Seq:       seq,
    Value:     v,
  }
  px.mu.Unlock()
  for index, peer := range(px.peers){
    var reply ReplyForAD
    if peer != px.peers[px.me] {
       call(peer, "Paxos.DecideReceive", args, &reply)
    } else {
      px.DecideReceive(args, &reply)
    }
    px.statusUpdate(reply.Done, index)
  }

  return nil
}

func (px *Paxos) AcceptReceive(args *AcceptArgs, reply *ReplyForAD) error {
  px.mu.Lock()
  defer px.mu.Unlock()
  //fmt.Printf("accept receive start\n")
  _, ok := px.instance[args.Seq]
  if !ok {
    px.instance[args.Seq] = &InstanceStatus{
      V_A:    nil,
      N_P:    0,
      N_A:    0,
      Finish: false,
    }
  }
  instance := px.instance[args.Seq]
  //fmt.Printf("%d, %d\n",args.Timestamp, instance.N_P)
  if args.Timestamp >= instance.N_P {
    reply.Error = false
    instance.V_A = args.Value
    instance.N_A = args.Timestamp
    instance.N_P = args.Timestamp
  } else {
    reply.Error = true
  }

  reply.Done = px.dones[px.me]
  return nil
}

func (px *Paxos) statusUpdate(seq int, index int) {
  if seq < 0 {
    return
  }
  px.mu.Lock()
  defer px.mu.Unlock()
  px.dones[index] = seq
  min := px.dones[px.me]
  for _, done := range px.dones {
    if done < min {
      min = done
    }
  }
  px.min = min + 1
}

func (px *Paxos) ErrorFree(seq int) bool {
  px.mu.Lock()
  defer px.mu.Unlock()
  if instance, ok := px.instance[seq]; ok && instance.Finish || seq < px.min {
    return false
  } else {
    return true
  }
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  px := &Paxos{}
  px.peers = peers
  px.me = me


  // Your initialization code here.
  px.instance = make(map[int]*InstanceStatus)
  px.dones = make([]int, len(px.peers))
  for i:=0; i < len(peers); i++{
    px.dones[i] = -1
  }
  px.min = -1
  px.max = -1

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l

    // please do not change any of the following code,
    // or do anything to subvert it.

    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }


  return px
}

