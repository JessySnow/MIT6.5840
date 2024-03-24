package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

var pingGap = 2 * time.Second
var workerId int

type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// ByKey KeyValue 排序接口实现
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// 0. 加入调度
	id, ok := joinCoordinator()
	if !ok {
		log.Fatal("Join Coordinator failed!")
	}
	workerId = id
	log.SetPrefix("[Worker#" + strconv.Itoa(workerId) + "]: ")

	// 1. Worker 保活
	go func() {
		for {
			if !pingCoordinator(workerId) {
				log.Fatal("Disconnected from coordinator, worker exit!")
			}
			time.Sleep(pingGap)
		}
	}()

	// 2. Worker 执行工作负载
	for {
		// 执行工作负载
		if t, e := doWorkLoad(mapf, reducef); e == nil {
			// 提交任务
			submitTask(*t)
		} else {
			// 输出异常信息
			log.Printf("%v\n", e)
		}
		time.Sleep(2 * time.Second)
	}
}

// 加入调度器
func joinCoordinator() (id int, ok bool) {
	ok = call("Coordinator.Join", struct{}{}, &id)
	return
}

// 从调度器获取任务
func fetchTask() (task Task, ok bool) {
	ok = call("Coordinator.FetchTask", struct{}{}, &task)
	return
}

// Ping 调度器进行保活
func pingCoordinator(wid int) (ok bool) {
	ok = call("Coordinator.Ping", wid, &struct{}{})
	return
}

// 提交完成的任务
func submitTask(tr Task) (ok bool) {
	ok = call("Coordinator.SubmitTask", tr, &struct{}{})
	return
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// 将 Map 方法所产生的中间键按照 key 的 hash 值存储为不同的文件
func saveKeyValueToFile(kvs []KeyValue, nReduce int) (onames []string, err error) {
	midKeyValuesMap := make(map[int][]KeyValue)

	// 根据哈希结果将键分配到不同的桶中
	for _, v := range kvs {
		midKey := ihash(v.Key) % nReduce

		if vs, ok := midKeyValuesMap[midKey]; !ok {
			midKeyValuesMap[midKey] = []KeyValue{v}
		} else {
			vs = append(vs, v)
			midKeyValuesMap[midKey] = vs
		}
	}

	// 将桶中的中间键持久化到文件中，并返回文件的名称
	i := 0
	onames = make([]string, len(midKeyValuesMap))
	for k, v := range midKeyValuesMap {
		fileName := "worker-" + strconv.Itoa(workerId) + "-" + time.Now().String() + "-intermediate-" + strconv.Itoa(k)
		f, e := os.Create(fileName)
		if e != nil {
			return
		}

		bytes, _ := json.Marshal(v)

		_, e = f.Write(bytes)
		if e != nil {
			return
		}

		onames[i] = fileName
		i += 1
	}

	return
}

// 从中间键文件中恢复出 KeyValue 切片
func restoreKeyValueFromFiles(files []string) (kvs []KeyValue, err error) {
	for _, f := range files {
		bytes, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}

		_kvs := make([]KeyValue, 0)
		err = json.Unmarshal(bytes, &_kvs)
		if err != nil {
			return nil, err
		}

		kvs = append(kvs, _kvs...)
	}

	return
}

// 执行工作负载
func doWorkLoad(mapf func(string, string) []KeyValue, reducef func(string, []string) string) (t *Task, err error) {

	// 0. 获取任务
	task, ok := fetchTask()
	if !ok || task.Type == UnDefined {
		return nil, fmt.Errorf("unsupported task type")
	}

	// 0. 构造任务执行的响应体
	tr := Task{Type: task.Type, Data: make(map[TaskParam]interface{})}
	tr.Data[WorkerId] = workerId
	tr.Data[TaskId] = task.Data[TaskId]

	// 1. 执行任务
	switch task.Type {
	case MapTask:
		iname := task.Data[MapTaskInputFilePath].(string)
		nReduce := task.Data[ReduceNum].(int)
		contents, err := os.ReadFile(iname)
		if err != nil {
			log.Println("Read input file failed!")
			return nil, err
		}

		kvs := mapf(iname, string(contents))
		files, err := saveKeyValueToFile(kvs, nReduce)
		if err != nil {
			log.Println("Save intermediate to file failed!")
			return nil, err
		}

		tr.Data[MapTaskOutPutFilePath] = files
	case ReduceTask:
		inames := task.Data[ReduceTaskInputFiles].([]string)
		oname := "mr-out-" + strconv.Itoa(task.Data[ReduceTaskKey].(int))
		ofile, err := os.Create(oname)
		if err != nil {
			log.Println("Create reduce output file failed!")
			return nil, err
		}

		intermediate, err := restoreKeyValueFromFiles(inames)
		if err != nil {
			log.Println("Restore intermediate from file failed!")
			return nil, err
		}

		sort.Sort(ByKey(intermediate))
		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			var values []string
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			output := reducef(intermediate[i].Key, values)

			// this is the correct format for each line of Reduce output.
			_, err := fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
			if err != nil {
				log.Println("Write reduce result to file failed")
				return nil, err
			}

			i = j
		}
	}

	return &tr, nil
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
