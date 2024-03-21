package mr

import (
	"fmt"
	"os"
	"sort"
	"sync"
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
	Key   string
	Value string
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

	// 保活
	go func() {
		for {
			pingCoordinator(wid)
			time.Sleep(2 * time.Second)
		}
	}()

	wg := sync.WaitGroup{}
	wg.Add(1)
	// 开启请求并执行任务
	go func() {
		for {
			time.Sleep(5 * time.Second)

			// 0. 获取任务
			task, ok := fetchTask(wid)
			if !ok || task.Type == UnDefined {
				continue
			}

			// 1. 执行任务
			ret := TaskResp{Type: task.Type}
			switch task.Type {
			case MapTask:
				inputFileName := task.Param[InputFilePath].(string)
				contents, err := os.ReadFile(inputFileName)
				if err != nil {
					continue
				}

				kvs := mapf(inputFileName, string(contents))
				files, err := saveKeyValueToFile(kvs)
				if err != nil {
					log.Printf("Save midkey to file failed!")
					continue
				}

				ret.Resp[OutPutFilePath] = files
			case ReduceTask:
				inputFileNames := task.Param[ReduceTaskInputFiles].([]string)
				oname := task.Param[ReduceTaskOutPutFile].(string)
				ofile, _ := os.Create(oname)
				intermediate, err := restoreKeyValueFromFiles(inputFileNames)
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
	}()

	wg.Wait()
}

func joinCoordinator() (id int, ok bool) {
	ok = call("Coordinator.Join", struct{}{}, &id)
	return
}

func fetchTask(wid int) (task TaskReq, ok bool) {
	ok = call("Coordinator.FetchTask", wid, &task)
	return
}

func pingCoordinator(wid int) (ok bool) {
	ok = call("Coordinator.Ping", wid, struct{}{})
	return
}

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
func saveKeyValueToFile(kvs []KeyValue) (fileNames []string, err error) {
	return
}

// 从中间键文件中恢复出 KeyValue 切片
func restoreKeyValueFromFiles(files []string) (kvs []KeyValue, err error) {
	return
}
