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
	WorkerExpireTime = 5 * time.Second
	TaskTimeOutTime  = 10 * time.Second
	UnDefined        = 0
	NotStarted       = 1
	Started          = 2
	Done             = 3
)

// 全局变量定义
var (
	// worker map 和相关的访问 channel
	workers        = make(map[int]worker)
	workIdInChan   = make(chan int)
	workIdsOutChan = make(chan []int)

	// mapTask map 和相关的访问 channel
	mapTasks          = make(map[int]mapTask)
	fetchMapTaskChan  = make(chan struct{})
	returnMapTaskChan = make(chan mapTask)
	updateMapTaskChan = make(chan []mapTask)

	// reduceTask map 和相关的访问 channel
	reduceTasks          = make(map[int]reduceTask)
	fetchReduceTaskChan  = make(chan struct{})
	returnReduceTaskChan = make(chan reduceTask)
	updateReduceTaskChan = make(chan []reduceTask)
)

type worker struct {
	id           int
	lastPingTime time.Time
}

type task struct {
	id, status  int
	refreshTime time.Time
}

func (t task) isZero() bool {
	return t.status == UnDefined
}

type mapTask struct {
	task
	inputFilePath string
}

type reduceTask struct {
	task
	outputFilePath string
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
	go mapTaskSelect()

	c.server()
	return &c
}

// workersSelect 针对 workers 的访问和更新操作代码块
func workersSelect() {
	for {
		select {
		// 新的 worker 加入，或者老的 worker 进行保活
		case wid := <-workIdInChan:
			if v, ok := workers[wid]; !ok {
				workers[wid] = worker{id: wid, lastPingTime: time.Now()}
			} else {
				v.lastPingTime = time.Now()
			}
		// worker 保活超时检查
		case <-time.Tick(WorkerExpireTime):
			ret := make([]int, 0)
			for k, v := range workers {
				if time.Now().Sub(v.lastPingTime).Seconds() > float64(WorkerExpireTime) {
					delete(workers, k)
					ret = append(ret, k)
				}
			}
			// 发送 worker 失效通知
			if len(ret) != 0 {
				workIdsOutChan <- ret
			}
		}
	}
}

// mapTaskSelect 针对 mapTask 的访问和更新代码块
func mapTaskSelect() {
	for {
		select {
		// 尝试获取 mapTask
		case <-fetchMapTaskChan:
			for _, v := range mapTasks {
				if v.status == NotStarted {
					v.status = Started
					v.refreshTime = time.Now()
					returnMapTaskChan <- v
					break
				}
			}
			returnMapTaskChan <- mapTask{}
		// 尝试更新 mapTask
		case mts := <-updateMapTaskChan:
			for _, mt := range mts {
				if v, ok := mapTasks[mt.id]; ok {
					if mt.status != UnDefined {
						v.status = mt.status
					}
					if !mt.refreshTime.IsZero() {
						v.refreshTime = mt.refreshTime
					}
				}
			}
		// 遍历 mapTask，将过期的任务重新设置为未开始的状态
		case <-time.Tick(TaskTimeOutTime):
			now := time.Now()
			for _, v := range mapTasks {
				if v.status == Started && now.Sub(v.refreshTime).Seconds() > float64(TaskTimeOutTime) {
					v.status = NotStarted
				}
			}
		}
	}
}
