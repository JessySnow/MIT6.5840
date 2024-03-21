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
	globalWorkerIdCounter = 0
	workers               = make(map[int]worker)
	refreshWorkerChan     = make(chan int)
	fetchWorkerIdChan     = make(chan struct{})
	returnWorkerIdChan    = make(chan int)

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

	// worker 和已完成的 mapTaskId 之间的映射关系
	workerMapTaskLists      = make(map[int][]int)
	addDoneTaskChan         = make(chan workerMapTaskPair)
	expireWorkerAndTaskChan = make(chan []int)
	expireTaskChan          = make(chan []int)

	// worker 和其输出的中间键的文件位置
	workerMidKeyFiles             = make(map[int][]string)
	addWorkerMidKeyFilesChan      = make(chan workerMidKeyFilePair)
	expireWorkerAndMidKeyFileChan = make(chan []int)
)

type worker struct {
	id          int
	refreshTime time.Time
}

type task struct {
	id, status  int
	refreshTime time.Time
}

type workerMapTaskPair struct {
	wid, tid int
}

type workerMidKeyFilePair struct {
	wid   int
	files []string
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

// Ping worker 保活接口
func (c *Coordinator) Ping(wid int, ret *struct{}) error {
	refreshWorkerChan <- wid
	return nil
}

// Join worker 加入到 coordinator，返回 worker 的 id
func (c *Coordinator) Join(param struct{}, wid *int) error {
	fetchWorkerIdChan <- param
	*wid = <-returnWorkerIdChan
	return nil
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

	// 初始化 MapTask
	for i, f := range files {
		m := mapTask{task{id: i, status: NotStarted, refreshTime: time.Now()}, f}
		mapTasks[i] = m
	}

	// Your code here.
	go workersHandler()
	go mapTaskHandler()
	go reduceTaskHandler()
	go workerMapTaskListsHandler()
	go workerMidKeyFilesHandler()

	c.server()
	return &c
}

// workersSelect 针对 workers 的访问和更新操作代码块
func workersHandler() {
	for {
		select {
		// 老 worker 保活
		case wid := <-refreshWorkerChan:
			if v, ok := workers[wid]; ok {
				v.refreshTime = time.Now()
			}
		// 新 worker 加入
		case <-fetchWorkerIdChan:
			wid := globalWorkerIdCounter
			globalWorkerIdCounter++
			workers[wid] = worker{id: wid, refreshTime: time.Now()}
			returnWorkerIdChan <- wid
		// worker 保活超时检查
		case <-time.Tick(WorkerExpireTime):
			ret := make([]int, 0)
			now := time.Now()
			for k, v := range workers {
				if now.Sub(v.refreshTime).Seconds() > float64(WorkerExpireTime) {
					delete(workers, k)
					ret = append(ret, k)
				}
			}
			// 发送 worker 失效通知
			if len(ret) != 0 {
				expireWorkerAndMidKeyFileChan <- ret
				expireWorkerAndTaskChan <- ret
			}
		}
	}
}

// mapTaskSelect 针对 mapTask 的访问和更新代码块
func mapTaskHandler() {
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
		// 过期导致任务重置
		case ets := <-expireTaskChan:
			for _, et := range ets {
				if v, ok := mapTasks[et]; ok {
					v.status = NotStarted
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

// reduceTaskSelect 针对 reduceTask 的访问和更新代码块
func reduceTaskHandler() {
	for {
		select {
		// 尝试获取 reduceTasks
		case <-fetchReduceTaskChan:
			for _, v := range reduceTasks {
				if v.status == NotStarted {
					v.status = Started
					v.refreshTime = time.Now()
					returnReduceTaskChan <- v
					break
				}
			}
			returnReduceTaskChan <- reduceTask{}
		// 尝试更新 reduceTasks
		case rts := <-updateReduceTaskChan:
			for _, rt := range rts {
				if v, ok := reduceTasks[rt.id]; ok {
					if rt.status != UnDefined {
						v.status = rt.status
					}
					if !rt.refreshTime.IsZero() {
						v.refreshTime = rt.refreshTime
					}
					if rt.outputFilePath != "" {
						v.outputFilePath = rt.outputFilePath
					}
				}
			}
		// 遍历 reduceTasks 将过期的任务重新设置为未开始的状态
		case <-time.Tick(TaskTimeOutTime):
			now := time.Now()
			for _, v := range reduceTasks {
				if v.status == Started && now.Sub(v.refreshTime).Seconds() > float64(TaskTimeOutTime) {
					v.status = NotStarted
				}
			}
		}
	}
}

// workerMapTaskListsSelect 针对 workerMapTaskListsSelect 的访问和更新代码
func workerMapTaskListsHandler() {
	for {
		select {
		// 有新任务完成
		case pair := <-addDoneTaskChan:
			if v, ok := workerMapTaskLists[pair.wid]; !ok {
				workerMapTaskLists[pair.wid] = []int{pair.tid}
			} else {
				v = append(v, pair.tid)
			}
		// 因为 worker 过期导致 mapTask 需要重做
		case ws := <-expireWorkerAndTaskChan:
			ret := make([]int, 0)
			for _, w := range ws {
				ret = append(ret, workerMapTaskLists[w]...)
			}
			expireTaskChan <- ret
		}
	}
}

// workerMidKeyFilesSelect 针对 workerMidKeyFiles 的访问和更新代码
func workerMidKeyFilesHandler() {
	for {
		select {
		case pair := <-addWorkerMidKeyFilesChan:
			if v, ok := workerMidKeyFiles[pair.wid]; !ok {
				workerMidKeyFiles[pair.wid] = pair.files
			} else {
				v = append(v, pair.files...)
			}
		case ids := <-expireWorkerAndMidKeyFileChan:
			for _, id := range ids {
				delete(workerMidKeyFiles, id)
			}
		}
	}
}
