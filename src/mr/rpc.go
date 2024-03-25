package mr

import "os"
import "strconv"

type TaskParam int
type TaskType int

// 任务类型
const (
	UnDefined TaskType = iota
	MapTask
	ReduceTask
)

// 任务参数
const (
	TaskId TaskParam = iota
	WorkerId
	ReduceNum
	MapTaskInputFilePath
	MapTaskOutPutFilePath
	ReduceTaskInputFiles
)

type Task struct {
	Type TaskType
	Data map[TaskParam]interface{}
}

func (t Task) isZero() bool {
	return t.Type == UnDefined
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
