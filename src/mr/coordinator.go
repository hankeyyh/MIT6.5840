package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"

	"6.5840/mylog"
)

type Coordinator struct {
	// Your definitions here.
	fileList []string
	mu       sync.RWMutex

	// map任务状态 0-未启动，1-运行中，2-已完成
	mapTaskState []int
	// map任务对应的term，用于超时控制
	mapTaskTerm []int
	// map任务数量
	mapTaskCnt int
	// map任务完成数量
	mapTaskFinished int

	// reduce任务状态 0-未启动，1-运行中，2-已完成
	reduceTaskState []int
	// reduce任务对应的term，用于超时控制
	reduceTaskTerm []int
	// reduce任务数量
	reduceTaskCnt int
	// reduce任务完成数量
	reduceTaskFinished int
}

func NewCoordinator(fileList []string, mapCnt int, reduceCnt int) *Coordinator {
	return &Coordinator{
		fileList: fileList,

		mapTaskState:    make([]int, mapCnt),
		mapTaskTerm:     make([]int, mapCnt),
		mapTaskCnt:      mapCnt,
		mapTaskFinished: 0,

		reduceTaskState:    make([]int, reduceCnt),
		reduceTaskTerm:     make([]int, reduceCnt),
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
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mapTaskFinished < c.mapTaskCnt {
		// map任务未完成，分配map任务
		return c.applyMapTask(args, reply)

	} else if c.mapTaskCnt == c.mapTaskFinished && c.reduceTaskFinished < c.reduceTaskCnt {
		// reduce任务未完成，分配reduce任务
		return c.applyReduceTask(args, reply)

	} else {
		// 任务已分配完毕
		mylog.Info("[coordinator]no task left")
		reply.TaskType = TaskTypeFinished
	}

	return nil
}

// 分配map任务
func (c *Coordinator) applyMapTask(args *ApplyTaskArgs, reply *ApplyTaskReply) error {
	taskId := c.nextMapTaskId()
	mylog.Infof("[coordinator]applyMapTask, taskId: %d", taskId)

	if taskId == -1 {
		reply.TaskType = TaskTypeWaiting
		return nil
	}
	c.mapTaskState[taskId] = 1
	c.mapTaskTerm[taskId]++

	reply.Filename = c.fileList[taskId]
	reply.TaskId = taskId
	reply.TaskTerm = c.mapTaskTerm[taskId]
	reply.TaskType = TaskTypeMap
	reply.MapTaskCnt = c.mapTaskCnt
	reply.ReduceTaskCnt = c.reduceTaskCnt // 每个map任务输出nReduce个文件

	// 若worker超时，则将taskState重新置为0，超时时间10s
	go func() {
		time.Sleep(time.Second * 10)
		c.resetMapTaskStateIfNotFinish(taskId)
	}()
	return nil
}

// 分配reduce任务
func (c *Coordinator) applyReduceTask(args *ApplyTaskArgs, reply *ApplyTaskReply) error {
	taskId := c.nextReduceTaskId()
	mylog.Infof("[coordinator]applyReduceTask, taskId: %d", taskId)

	if taskId == -1 {
		reply.TaskType = TaskTypeWaiting
		return nil
	}
	c.reduceTaskState[taskId] = 1
	c.reduceTaskTerm[taskId]++

	reply.TaskId = taskId
	reply.TaskTerm = c.reduceTaskTerm[taskId]
	reply.TaskType = TaskTypeReduce
	reply.MapTaskCnt = c.mapTaskCnt
	reply.ReduceTaskCnt = c.reduceTaskCnt

	// 若worker超时，则将taskState重新置为0，超时时间10s
	go func() {
		time.Sleep(time.Second * 10)
		c.resetReduceTaskStateIfNotFinish(taskId)
	}()
	return nil
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	mylog.Infof("[coordinator]FinishTask, taskType: %d, taskId: %d", args.TaskType, args.TaskId)

	if args.TaskType == TaskTypeMap && c.mapTaskTerm[args.TaskId] == args.TaskTerm && c.mapTaskState[args.TaskId] == 1 {
		c.mapTaskState[args.TaskId] = 2
		c.mapTaskFinished++
	} else if args.TaskType == TaskTypeReduce && c.reduceTaskTerm[args.TaskId] == args.TaskTerm && c.reduceTaskState[args.TaskId] == 1 {
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
	if c.reduceTaskCnt == c.reduceTaskFinished {
		mylog.Info("[coordinator]Done!")
	}
	return c.reduceTaskCnt == c.reduceTaskFinished
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	mylog.Infof("[coordinator]Start MakeCoordinator, mapTaskCnt: %d, reduceTaskCnt: %d", len(files), nReduce)
	c := NewCoordinator(files, len(files), nReduce)

	c.server()
	return c
}
