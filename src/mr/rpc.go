package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add your RPC definitions here.
type TType int

const (
	TaskTypeNone   TType = 0
	TaskTypeMap    TType = 1
	TaskTypeReduce TType = 2
	TaskTypeWaiting TType = 3 // 等待任务完成
	TaskTypeFinished TType = 4 // 全部任务完成
)

type ApplyTaskArgs struct {
}

type ApplyTaskReply struct {
	TaskType TType
	TaskId   int
	TaskTerm int
	Filename string
	MapTaskCnt int
	ReduceTaskCnt int
}

type FinishTaskArgs struct {
	TaskType TType
	TaskId   int
	TaskTerm int
}

type FinishTaskReply struct {
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
