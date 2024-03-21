package mr

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// KeyValue Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	wid, ok := joinCoordinator()
	if !ok {
		log.Fatal("Join Coordinator failed!")
	}

	// 每 2s Ping 一次 Coordinator 进行保活
	go func() {
		for {
			if !pingCoordinator(wid) {
				log.Fatal("Disconnected from coordinator worker exit!")
			}
			time.Sleep(2 * time.Second)
		}
	}()

	for {
		time.Sleep(5 * time.Second)

		// 0. 获取任务
		task, ok := fetchTask(wid)
		if !ok || task.Type == UnDefined {
			continue
		}

		// 0. 准备任务执行的响应体
		ret := TaskResp{Type: task.Type}
		ret.Resp[WorkerId] = wid
		ret.Resp[TaskId] = task.Param[TaskId]

		// 1. 执行任务
		switch task.Type {
		case MapTask:
			iname := task.Param[MapTaskInputFilePath].(string)
			nReduce := task.Param[ReduceNum].(int)
			contents, err := os.ReadFile(iname)
			if err != nil {
				continue
			}

			kvs := mapf(iname, string(contents))
			files, err := saveKeyValueToFile(kvs, nReduce)
			if err != nil {
				log.Printf("Save midkey to file failed!")
				continue
			}

			ret.Resp[MapTaskOutPutFilePath] = files
		case ReduceTask:
			iname := task.Param[ReduceTaskInputFiles].([]string)
			oname := "mr-out-" + strconv.Itoa(task.Param[ReduceTaskKey].(int))
			ofile, err := os.Create(oname)
			if err != nil {
				continue
			}
			intermediate, err := restoreKeyValueFromFiles(iname)
			if err != nil {
				continue
			}

			sort.Sort(ByKey(intermediate))
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
		}

		// 3. 提交任务
		submitTask(wid, ret)
	}
}

// 加入调度器
func joinCoordinator() (id int, ok bool) {
	ok = call("Coordinator.Join", struct{}{}, &id)
	return
}

// 从调度器获取任务
func fetchTask(wid int) (task TaskReq, ok bool) {
	ok = call("Coordinator.FetchTask", wid, &task)
	return
}

// Ping 调度器进行保活
func pingCoordinator(wid int) (ok bool) {
	ok = call("Coordinator.Ping", wid, struct{}{})
	return
}

// 提交完成的任务
func submitTask(wid int, tp TaskResp) (ok bool) {
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
func saveKeyValueToFile(kvs []KeyValue, nReduce int) (fileNames []string, err error) {
	midKeyValuesMap := make(map[int][]KeyValue)

	// 根据哈希结果将键分配到不同的桶中
	for _, v := range kvs {
		midKey := ihash(v.Key) % nReduce

		if vs, ok := midKeyValuesMap[midKey]; !ok {
			midKeyValuesMap[midKey] = []KeyValue{v}
		} else {
			vs = append(vs, v)
		}
	}

	// 将桶中的中间键持久化到文件中，并返回文件的名称
	fileNames = make([]string, len(midKeyValuesMap))
	for k, v := range midKeyValuesMap {
		fileName := "intermediate-" + strconv.Itoa(k)
		f, err := os.Create(fileName)
		if err != nil {
			return
		}

		bytes, err := json.Marshal(v)
		if err != nil {
			return
		}

		_, err = f.Write(bytes)
		if err != nil {
			return
		}

		fileNames = append(fileNames, fileName)
	}

	return
}

// 从中间键文件中恢复出 KeyValue 切片
func restoreKeyValueFromFiles(files []string) (kvs []KeyValue, err error) {
	for _, f := range files {
		bytes, err := os.ReadFile(f)
		if err != nil {
			return
		}

		_kvs := make([]KeyValue, 0)
		err = json.Unmarshal(bytes, &_kvs)
		if err != nil {
			return
		}

		kvs = append(kvs, _kvs...)
	}

	return
}
