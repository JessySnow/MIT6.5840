package mr

import (
	"fmt"
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// 全局常量定义
const (
	workerExpireTime = 5 * time.Second
	taskTimeOutTime  = 10 * time.Second
	unDefined        = 0
	notStarted       = 1
	started          = 2
	done             = 3
)

// 全局变量定义
var (
	// worker map 和相关的访问 channel
	widCounter         = 0
	workers            = make(map[int]worker)
	refreshWorkerChan  = make(chan int)
	fetchWorkerIdChan  = make(chan struct{})
	returnWorkerIdChan = make(chan int)

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

	// reduce 任务的个数
	_nReduce int
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
	return t.status == unDefined
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

// FetchTask 获取任务
func (c *Coordinator) FetchTask(param struct{}, task *Task) error {
	// 0. 获取 Map 任务
	fetchMapTaskChan <- struct{}{}
	if mt := <-returnMapTaskChan; !mt.isZero() {
		task.Data = make(map[TaskParam]interface{})
		task.Type = MapTask
		task.Data[MapTaskInputFilePath] = mt.inputFilePath
		task.Data[TaskId] = mt.id
		task.Data[ReduceNum] = _nReduce
		return nil
	}

	// 1. 获取 Reduce 任务
	fetchReduceTaskChan <- struct{}{}
	if rt := <-returnReduceTaskChan; !rt.isZero() {
		task.Type = ReduceTask
		// TODO
		task.Data[ReduceTaskInputFiles] = nil
		task.Data[TaskId] = rt.id
		return nil
	}

	// 2. 没有任务可以获取
	return nil
}

// SubmitTask 提交任务
func (c *Coordinator) SubmitTask(param Task, ret *struct{}) error {
	pwid := param.Data[WorkerId].(int)
	ptid := param.Data[TaskId].(int)
	oname := param.Data[MapTaskOutPutFilePath].([]string)

	switch param.Type {
	case MapTask:
		// 更新任务执行情况
		updateMapTaskChan <- []mapTask{{task: task{id: pwid, status: done}}}
		// 更新 worker 和任务执行的对应关系
		addDoneTaskChan <- workerMapTaskPair{pwid, ptid}
		// 更新 worker 和 map 任务中间键输出地址的关系
		addWorkerMidKeyFilesChan <- workerMidKeyFilePair{wid: pwid, files: oname}
	case ReduceTask:
		// 更新任务执行情况
		updateReduceTaskChan <- []reduceTask{{task: task{id: pwid, status: done}}}
	}

	return nil
}

// Your code here -- RPC handlers for the worker to call.

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
		m := mapTask{task{id: i, status: notStarted}, f}
		mapTasks[i] = m
	}
	_nReduce = nReduce

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
			wid := widCounter
			widCounter++
			workers[wid] = worker{id: wid, refreshTime: time.Now()}
			returnWorkerIdChan <- wid
		// worker 保活超时检查
		case <-time.Tick(workerExpireTime):
			ret := make([]int, 0)
			now := time.Now()
			for k, v := range workers {
				if now.Sub(v.refreshTime).Seconds() > float64(workerExpireTime) {
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
		// 获取 mapTask
		case <-fetchMapTaskChan:
			fmt.Println("FetchTask")
			sented := false
			for _, v := range mapTasks {
				if v.status == notStarted {
					sented = true
					v.status = started
					v.refreshTime = time.Now()
					returnMapTaskChan <- v
					break
				}
			}
			if !sented {
				returnMapTaskChan <- mapTask{}
			}
		// 更新 mapTask
		case mts := <-updateMapTaskChan:
			fmt.Println("UpdateTask")
			for _, mt := range mts {
				if v, ok := mapTasks[mt.id]; ok {
					if mt.status != unDefined {
						v.status = mt.status
					}
					if !mt.refreshTime.IsZero() {
						v.refreshTime = mt.refreshTime
					}
				}
			}
		// 过期导致任务重置
		case ets := <-expireTaskChan:
			fmt.Println("ExpireTask")
			for _, et := range ets {
				if v, ok := mapTasks[et]; ok {
					v.status = notStarted
				}
			}
		// 遍历 mapTask，将过期的任务重新设置为未开始的状态
		case <-time.Tick(taskTimeOutTime):
			fmt.Println("ExpireTask")
			now := time.Now()
			for _, v := range mapTasks {
				if v.status == started && now.Sub(v.refreshTime).Seconds() > float64(taskTimeOutTime) {
					v.status = notStarted
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
				if v.status == notStarted {
					v.status = started
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
					if rt.status != unDefined {
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
		case <-time.Tick(taskTimeOutTime):
			now := time.Now()
			for _, v := range reduceTasks {
				if v.status == started && now.Sub(v.refreshTime).Seconds() > float64(taskTimeOutTime) {
					v.status = notStarted
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
