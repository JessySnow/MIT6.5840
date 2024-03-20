package mr

import (
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// 全局常量定义
const (
	WORKER_EXPIRE_TIME = 5 * time.Second
)

// 全局变量定义
var (
	// worker 列表和相关的访问 channel
	workers         = make(map[int]worker)
	workIdInChain   = make(chan int)
	workIdsOutChain = make(chan []int)
)

type worker struct {
	id           int
	lastPingTime time.Time
}

type Coordinator struct {
	// Your definitions here.
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	go workersSelect()

	c.server()
	return &c
}

// workersSelect 针对 workers 的访问和更新操作代码块
func workersSelect() {
	for {
		select {
		// 新的 worker 加入，或者老的 worker 进行保活
		case wid := <-workIdInChain:
			if v, ok := workers[wid]; !ok {
				workers[wid] = worker{id: wid, lastPingTime: time.Now()}
			} else {
				v.lastPingTime = time.Now()
			}
		// worker 保活超时检查
		case <-time.Tick(WORKER_EXPIRE_TIME):
			ret := make([]int, 0)
			for k, v := range workers {
				if time.Now().Sub(v.lastPingTime).Seconds() > float64(WORKER_EXPIRE_TIME) {
					delete(workers, k)
					ret = append(ret, k)
				}
			}
			// 发送 worker 失效通知
			if len(ret) != 0 {
				workIdsOutChain <- ret
			}
		}
	}
}
