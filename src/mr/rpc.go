package mr

import "os"
import "strconv"

const (
	UnDefined             TaskType  = 0
	MapTask               TaskType  = 1
	ReduceTask            TaskType  = 2
	MapTaskInputFilePath  TaskParam = 1
	MapTaskOutPutFilePath TaskParam = 2
	ReduceNum             TaskParam = 3
	WorkerId              TaskParam = 4
	TaskId                TaskParam = 5
	ReduceTaskKey         TaskParam = 6
	ReduceTaskInputFiles  TaskParam = 7
)

type TaskParam int
type TaskType int

type TaskReq struct {
	Type  TaskType
	Param map[TaskParam]interface{}
}

type TaskResp struct {
	Type TaskType
	Resp map[TaskParam]interface{}
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
