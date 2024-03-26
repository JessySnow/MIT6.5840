package mr

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// 全局常量定义
const (
	workerExpireTime    = 5 * time.Second
	taskTimeOutTime     = 10 * time.Second
	jobDoneCheckGapTime = 1 * time.Second
	unDefined           = 0
	notStarted          = 1
	started             = 2
	done                = 3
)

var (
	// worker map 和相关的访问 channel
	widCounter         = 0
	workers            = make(map[int]worker)
	refreshWorkerChan  = make(chan int)
	fetchWorkerIdChan  = make(chan struct{})
	returnWorkerIdChan = make(chan int)

	// mapTask map 和相关的访问 channel
	mapTasks               = make(map[int]mapTask)
	fetchMapTaskChan       = make(chan struct{})
	returnMapTaskChan      = make(chan mapTask)
	submitMapTaskChan      = make(chan mapTaskExecResult)
	updateMapTaskChan      = make(chan []mapTask)
	checkMapTasksStatChan  = make(chan struct{})
	returnMapTasksStatChan = make(chan bool)

	// reduceTask map 和相关的访问 channel
	reduceTasks          = make(map[int]reduceTask)
	fetchReduceTaskChan  = make(chan struct{})
	returnReduceTaskChan = make(chan reduceTask)
	submitReduceTaskChan = make(chan reduceTask)

	// worker 和已完成的 mapTaskId 之间的映射关系
	workerMapTaskLists      = make(map[int][]int)
	addDoneTaskChan         = make(chan workerMapTaskPair)
	expireWorkerAndTaskChan = make(chan []int)

	// worker 和其输出的中间键的文件位置
	workerMidKeyFiles = make(map[int][]string)
	midKeyFileLock    sync.RWMutex

	// reduce 任务的个数
	_nReduce int
	// Coordinator 任务结束标志
	_doneLock sync.Mutex
	_done     bool
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

type mapTaskExecResult struct {
	wid, tid        int
	outputFilePaths []string
}

type reduceTask struct {
	task
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
		//log.Printf("MapTask#%d fetched", mt.id)
		return nil
	}

	// 检查 MapTask 是否全部已完成
	checkMapTasksStatChan <- struct{}{}
	allDone := <-returnMapTasksStatChan
	if !allDone {
		return nil
	}

	// 1. 获取 Reduce 任务
	fetchReduceTaskChan <- struct{}{}
	if rt := <-returnReduceTaskChan; !rt.isZero() {
		task.Data = make(map[TaskParam]interface{})
		task.Type = ReduceTask
		task.Data[ReduceTaskInputFiles] = getReduceInputFileNames(rt.id)
		task.Data[TaskId] = rt.id
		//log.Printf("ReduceTask#%d fetched", rt.id)
		return nil
	}

	// 2. 没有任务可以获取
	return nil
}

// SubmitTask 提交任务
func (c *Coordinator) SubmitTask(param Task, ret *struct{}) error {
	wid := param.Data[WorkerId].(int)
	tid := param.Data[TaskId].(int)

	switch param.Type {
	case MapTask:
		oname := param.Data[MapTaskOutPutFilePath].([]string)
		submitMapTaskChan <- mapTaskExecResult{wid, tid, oname}
		//log.Printf("worker#%d submit mapTask#%d", wid, tid)
	case ReduceTask:
		// 更新任务执行情况
		submitReduceTaskChan <- reduceTask{task{id: tid, status: done}}
		//log.Printf("worker#%d submit reduceTask#%d", wid, tid)
	default:
		panic("unhandled default case")
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
		//log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) Done() bool {
	_doneLock.Lock()
	defer _doneLock.Unlock()
	return _done
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// 初始化 Reduce 任务数量
	_nReduce = nReduce
	// 初始化 MapTask
	for i, f := range files {
		mapTasks[i] = mapTask{task{id: i, status: notStarted}, f}
	}
	// 初始化 ReduceTask
	for i := 0; i < _nReduce; i++ {
		reduceTasks[i] = reduceTask{task{id: i, status: notStarted}}
	}

	// Your code here.
	go workersHandler()
	go mapTaskHandler()
	go reduceTaskHandler()
	go workerMapTaskListsHandler()

	c.server()
	return &c
}

// workersSelect 针对 workers 的访问和更新操作代码块
func workersHandler() {
	ticker := time.NewTicker(workerExpireTime)

	for {
		select {
		// 老 worker 保活
		case wid := <-refreshWorkerChan:
			if v, ok := workers[wid]; ok {
				v.refreshTime = time.Now()
				workers[wid] = v
			}
		// 新 worker 加入
		case <-fetchWorkerIdChan:
			wid := widCounter
			widCounter++
			workers[wid] = worker{id: wid, refreshTime: time.Now()}
			returnWorkerIdChan <- wid
		// worker 超时检查
		case <-ticker.C:
			ret := make([]int, 0)
			now := time.Now()
			for k, v := range workers {
				if now.Sub(v.refreshTime).Seconds() > workerExpireTime.Seconds() {
					delete(workers, k)
					ret = append(ret, k)
				}
			}
			// 发送 worker 失效通知
			if len(ret) != 0 {
				expireWorkerAndMidKeyFile(ret)
				expireWorkerAndTaskChan <- ret
			}
		}
	}
}

// mapTaskSelect 针对 mapTask 的访问和更新代码块
func mapTaskHandler() {
	ticker := time.NewTicker(taskTimeOutTime)

	for {
		select {
		// 检查 mapTask 是否全部完成
		case <-checkMapTasksStatChan:
			ret := true
			for _, v := range mapTasks {
				if v.status != done {
					ret = false
					break
				}
			}
			returnMapTasksStatChan <- ret
		// 获取 mapTask
		case <-fetchMapTaskChan:
			ret := new(mapTask)
			for i, v := range mapTasks {
				if v.status == notStarted {
					v.status = started
					v.refreshTime = time.Now()
					mapTasks[i] = v
					*ret = v
					break
				}
			}
			returnMapTaskChan <- *ret
		// 更新 mapTask
		case mts := <-updateMapTaskChan:
			for _, mt := range mts {
				if v, ok := mapTasks[mt.id]; ok {
					if mt.status != unDefined {
						v.status = mt.status
					}
					if !mt.refreshTime.IsZero() {
						v.refreshTime = mt.refreshTime
					}
					mapTasks[mt.id] = v
				}
			}
		// 提交任务
		case result := <-submitMapTaskChan:
			if v, ok := mapTasks[result.tid]; ok && v.status != done {
				v.status = done
				mapTasks[result.tid] = v
				// 更新 worker 和任务执行的对应关系
				addDoneTaskChan <- workerMapTaskPair{result.wid, result.tid}
				// 更新 worker 和 map 任务中间键输出地址的关系
				addWorkerMidKeyFiles(workerMidKeyFilePair{result.wid, result.outputFilePaths})
			}
		// 遍历 mapTask，将过期的任务重新设置为未开始的状态
		case <-ticker.C:
			now := time.Now()
			for k, v := range mapTasks {
				if v.status == started && now.Sub(v.refreshTime).Seconds() > taskTimeOutTime.Seconds() {
					v.status = notStarted
					mapTasks[k] = v
				}
			}
		}
	}
}

// reduceTaskSelect 针对 reduceTask 的访问和更新代码块
func reduceTaskHandler() {
	ticker := time.NewTicker(taskTimeOutTime)
	doneTicker := time.NewTicker(jobDoneCheckGapTime)

	for {
		select {
		// 尝试获取 reduceTasks
		case <-fetchReduceTaskChan:
			ret := new(reduceTask)
			for k, v := range reduceTasks {
				if v.status == notStarted {
					v.status = started
					v.refreshTime = time.Now()
					reduceTasks[k] = v
					*ret = v
					break
				}
			}
			returnReduceTaskChan <- *ret
		// 提交 reduceTasks
		case rt := <-submitReduceTaskChan:
			if v, ok := reduceTasks[rt.id]; ok && v.status != done {
				v.status = done
				reduceTasks[rt.id] = v
			}
		// 遍历 reduceTasks 将过期的任务重新设置为未开始的状态
		case <-ticker.C:
			now := time.Now()
			for k, v := range reduceTasks {
				if v.status == started && now.Sub(v.refreshTime).Seconds() > taskTimeOutTime.Seconds() {
					v.status = notStarted
					reduceTasks[k] = v
					fmt.Println(v)
				}
			}
		case <-doneTicker.C:
			tag := true
			for _, v := range reduceTasks {
				if v.status != done {
					tag = false
					break
				}
			}
			if tag {
				_doneLock.Lock()
				_done = true
				_doneLock.Unlock()
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
				workerMapTaskLists[pair.wid] = v
			}
		// 因为 worker 过期导致 mapTask 需要重做
		case ws := <-expireWorkerAndTaskChan:
			ret := make([]mapTask, 0)
			for _, w := range ws {
				if tids, ok := workerMapTaskLists[w]; ok {
					for _, tid := range tids {
						ret = append(ret, mapTask{task: task{id: tid, status: notStarted}})
					}
					delete(workerMapTaskLists, w)
				}
			}
			updateMapTaskChan <- ret
		}
	}
}

// addWorkerMidKeyFiles 新增 worker 和中间键输出文件的映射
func addWorkerMidKeyFiles(p workerMidKeyFilePair) {
	midKeyFileLock.Lock()
	defer midKeyFileLock.Unlock()
	if v, ok := workerMidKeyFiles[p.wid]; !ok {
		workerMidKeyFiles[p.wid] = p.files
	} else {
		v = append(v, p.files...)
		workerMidKeyFiles[p.wid] = v
	}
}

// expireWorkerAndMidKeyFile 对过期的 worker 进行清理
func expireWorkerAndMidKeyFile(wids []int) {
	midKeyFileLock.Lock()
	defer midKeyFileLock.Unlock()
	for _, id := range wids {
		delete(workerMidKeyFiles, id)
	}
}

// getReduceInputFileNames 根据 Reduce 任务的编号获取对应的中间键文件名
func getReduceInputFileNames(rid int) (ret []string) {
	midKeyFileLock.RLock()
	defer midKeyFileLock.RUnlock()
	for _, files := range workerMidKeyFiles {
		for _, file := range files {
			i := strings.LastIndex(file, "-") + 1
			if n, err := strconv.Atoi(file[i:]); err == nil && n == rid {
				ret = append(ret, file)
			}
		}
	}

	return
}
