package kvpaxos

import (
  "crypto/rand"
  "math/big"
  "net/rpc"
)
import "fmt"

type Clerk struct {
  servers []string
  // You will have to modify this struct.
  me int64
}
func nrand() int64 {
  max := big.NewInt(int64(1) << 62)
  bigx, _ := rand.Int(rand.Reader, max)
  x := bigx.Int64()
  return x
}

func MakeClerk(servers []string) *Clerk {
  ck := new(Clerk)
  ck.servers = servers
  // You'll have to add code here.
  ck.me = nrand()
  return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
          args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial("unix", srv)
  if errx != nil {
    return false
  }
  defer c.Close()

  err := c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
  args := &GetArgs{key, ck.me, nrand()}
  i := 0
  var reply GetReply
  ok := call(ck.servers[i], "KVPaxos.Get", args, &reply)
  for !ok && reply.Err != OK{
    if reply.Err == ErrNoKey{
      return ""
    }
    ok = call(ck.servers[(i + 1) % len(ck.servers)], "KVPaxos.Get", args, &reply)
  }
  return reply.Value

}

//
// set the value for a key.
// keeps trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
  args := &PutArgs{key, value, dohash, ck.me, nrand()}
  i := 0
  var reply PutReply
  ok := call(ck.servers[i], "KVPaxos.Put", args, &reply)
  for !ok {
    ok = call(ck.servers[(i + 1) % len(ck.servers)], "KVPaxos.Put", args, &reply)
  }
  return reply.PreviousValue
}

func (ck *Clerk) Put(key string, value string) {
  ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
  v := ck.PutExt(key, value, true)
  return v
}