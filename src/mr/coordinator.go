package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	fileList []string
	mu       sync.RWMutex

	// map任务状态 0-未启动，1-运行中，2-已完成
	mapTaskState []int
	// map任务数量
	mapTaskCnt int
	// map任务完成数量
	mapTaskFinished int

	// reduce任务状态 0-未启动，1-运行中，2-已完成
	reduceTaskState []int
	// reduce任务数量
	reduceTaskCnt int
	// reduce任务完成数量
	reduceTaskFinished int
}

func NewCoordinator(fileList []string, mapCnt int, reduceCnt int) *Coordinator {
	return &Coordinator{
		fileList: fileList,

		mapTaskState:    make([]int, mapCnt),
		mapTaskCnt:      mapCnt,
		mapTaskFinished: 0,

		reduceTaskState:    make([]int, reduceCnt),
		reduceTaskCnt:      reduceCnt,
		reduceTaskFinished: 0,
	}
}

func (c *Coordinator) nextMapTaskId() int {
	for i := range c.mapTaskState {
		if c.mapTaskState[i] == 0 {
			return i
		}
	}
	return -1
}

func (c *Coordinator) nextReduceTaskId() int {
	for i := range c.reduceTaskState {
		if c.reduceTaskState[i] == 0 {
			return i
		}
	}
	return -1
}

func (c *Coordinator) resetMapTaskStateIfNotFinish(i int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mapTaskState[i] == 1 {
		c.mapTaskState[i] = 0
	}
}

func (c *Coordinator) resetReduceTaskStateIfNotFinish(i int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.reduceTaskState[i] == 1 {
		c.reduceTaskState[i] = 0
	}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) ApplyTask(args *ApplyTaskArgs, reply *ApplyTaskReply) error {
	if c.mapTaskFinished < c.mapTaskCnt {
		// map任务未完成，分配map任务
		return c.applyMapTask(args, reply)

	} else if c.mapTaskCnt == c.mapTaskFinished && c.reduceTaskFinished < c.reduceTaskCnt {
		// reduce任务未完成，分配reduce任务
		return c.applyReduceTask(args, reply)

	} else {
		// 任务已分配完毕
		reply.TaskType = TaskTypeNone
	}

	return nil
}

// 分配reduce任务
func (c *Coordinator) applyReduceTask(args *ApplyTaskArgs, reply *ApplyTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	taskId := c.nextReduceTaskId()
	if taskId == -1 {
		reply.TaskType = TaskTypeNone
		return nil
	}
	c.reduceTaskState[taskId] = 1

	reply.TaskId = taskId
	reply.TaskType = TaskTypeReduce
	reply.MapTaskCnt = c.mapTaskCnt

	// 若worker超时，则将taskState重新置为0，超时时间10s
	go func() {
		time.Sleep(time.Second * 10)
		c.resetReduceTaskStateIfNotFinish(taskId)
	}()
	return nil
}

// 分配map任务
func (c *Coordinator) applyMapTask(args *ApplyTaskArgs, reply *ApplyTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	taskId := c.nextMapTaskId()
	if taskId == -1 {
		reply.TaskType = TaskTypeNone
		return nil
	}
	c.mapTaskState[taskId] = 1

	reply.Filename = c.fileList[taskId]
	reply.TaskId = taskId
	reply.TaskType = TaskTypeMap
	reply.ReduceTaskCnt = c.reduceTaskCnt // 每个map任务输出nReduce个文件

	// 若worker超时，则将taskState重新置为0，超时时间10s
	go func() {
		time.Sleep(time.Second * 10)
		c.resetMapTaskStateIfNotFinish(taskId)
	}()
	return nil
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == TaskTypeMap {
		c.mapTaskState[args.TaskId] = 2
		c.mapTaskFinished++
	} else if args.TaskType == TaskTypeReduce {
		c.reduceTaskState[args.TaskId] = 2
		c.reduceTaskFinished++
	}

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
	// reduce任务全部完成，coordinator退出
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.reduceTaskCnt == c.reduceTaskFinished
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := NewCoordinator(files, len(files), nReduce)

	c.server()
	return c
}
