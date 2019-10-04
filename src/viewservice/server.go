
package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  me string


  // Your declarations here.
  curView *View
  nextView *View
  timeStatus map[string]time.Time
  ack bool
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

  // Your code here.
  clerk, viewNum := args.Me, args.Viewnum
  // for starters, let server be primary
  if viewNum == 0  {
    if vs.curView == nil {
      vs.curView = &View{
        Viewnum: 1,
        Primary: clerk,
      }
    }
    vs.timeStatus[clerk] = time.Now()
  }

  if clerk == vs.curView.Primary {
    // primary ack
    //fmt.Printf("viewNum %d, curView vn %d, primary %s\n", viewNum, vs.curView.Viewnum, vs.curView.Primary)
    if viewNum == vs.curView.Viewnum {
      vs.ack = true
      vs.timeStatus[clerk] = time.Now()
    } else if viewNum == 0 {
      fmt.Printf("primary reboot\n")
      vs.nextView = &View{
        Viewnum: vs.curView.Viewnum+1,
        Primary: vs.curView.Backup,
        Backup:  "",
      }
    }
  } else if clerk == vs.curView.Backup {
    if viewNum == vs.curView.Viewnum {
      vs.timeStatus[clerk] = time.Now()
    }
  } else if vs.curView.Backup == ""{
    //fmt.Printf("idle becomes backup\n")
    vs.nextView = &View{
      Viewnum: vs.curView.Viewnum + 1,
      Primary: vs.curView.Primary,
      Backup:  clerk,
    }
  }


  reply.View = *vs.curView
  return nil
}

// 
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

  // Your code here.
  if vs.curView == nil {
    reply.View = View{0, "", ""}
  } else {
    reply.View = *vs.curView
  }
  return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

  // Your code here.
  for clerk, timesta := range vs.timeStatus {
    //fmt.Printf("%d, %d\n",PingInterval * DeadPings,  time.Now().Sub(timesta))

    if time.Now().Sub(timesta) >= PingInterval * DeadPings {
      //fmt.Printf("timeout for clerk %s\n", clerk)
      if clerk == vs.curView.Primary {
        if vs.curView.Backup != "" {

          vs.nextView = &View{
            Viewnum: vs.curView.Viewnum + 1,
            Primary: vs.curView.Backup,
            Backup:  "",
          }
        }
      } else if clerk == vs.curView.Backup {
        vs.nextView = &View{
          Viewnum: vs.curView.Viewnum + 1,
          Primary: vs.curView.Primary,
          Backup:  "",
        }
      }

    }
  }

  if vs.ack && vs.nextView != nil {
    //fmt.Printf("From %s, %s, %d\n", vs.curView.Primary, vs.curView.Backup, vs.curView.Viewnum)
    //fmt.Printf("To %s, %s, %d\n", vs.nextView.Primary, vs.nextView.Backup, vs.nextView.Viewnum)
    vs.curView = vs.nextView
    vs.nextView = nil
    vs.ack = false
  }
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
  vs.dead = true
  vs.l.Close()
}

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)
  vs.me = me
  // Your vs.* initializations here.
  vs.timeStatus = make(map[string]time.Time)
  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.dead == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.dead == false {
        fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.dead == false {
      vs.tick()
      time.Sleep(PingInterval)
    }
  }()

  return vs
}
