package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"

	"6.5840/mylog"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type KeyValueList []KeyValue

func (kvl KeyValueList) Len() int {
	return len(kvl)
}

func (kvl KeyValueList) Swap(i, j int) {
	kvl[i], kvl[j] = kvl[j], kvl[i]
}

func (kvl KeyValueList) Less(i, j int) bool {
	return kvl[i].Key < kvl[j].Key
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		// ask for task
		applyTaskArgs := &ApplyTaskArgs{}
		applyTaskReply := &ApplyTaskReply{}

		mylog.Infof("[worker %d]ApplyTask request", os.Getpid())
		call("Coordinator.ApplyTask", &applyTaskArgs, &applyTaskReply)
		mylog.Infof("[worker %d]ApplyTask result, taskId: %d, taskType: %d", os.Getpid(), applyTaskReply.TaskId, applyTaskReply.TaskType)

		if applyTaskReply.TaskType == TaskTypeFinished {
			// coornidator may died, which means job is finished
			break
		} else if applyTaskReply.TaskType == TaskTypeMap {
			filename := applyTaskReply.Filename
			taskId := applyTaskReply.TaskId
			taskTerm := applyTaskReply.TaskTerm
			reduceCnt := applyTaskReply.ReduceTaskCnt

			// 读取file
			fileContent, err := os.ReadFile(filename)
			if err != nil {
				fmt.Println(err)
				break
			}

			// mapf解析
			kvList := mapf(filename, string(fileContent))

			// 根据ihash(key)放入bucket
			bucketList := make([][]KeyValue, reduceCnt)
			for _, kv := range kvList {
				i := ihash(kv.Key) % reduceCnt
				bucketList[i] = append(bucketList[i], kv)
			}

			// 写入临时文件
			for i := 0; i < len(bucketList); i++ {
				ifilename := fmt.Sprintf("mr-%d-%d", taskId, i)
				ifile, err := os.CreateTemp("", ifilename+"*")
				if err != nil {
					fmt.Println(err)
					continue
				}
				enc := json.NewEncoder(ifile)
				for _, kv := range bucketList[i] {
					err = enc.Encode(kv)
					if err != nil {
						fmt.Println(err)
						continue
					}
				}
				os.Rename(ifile.Name(), ifilename)
				ifile.Close()
			}

			// 通知master任务完成
			taskFinishArgs := &FinishTaskArgs{TaskType: TaskTypeMap, TaskId: taskId, TaskTerm: taskTerm}
			taskFinishReply := &FinishTaskReply{}

			mylog.Infof("[worker %d]FinishMapTask request, taskId: %d", os.Getpid(), taskFinishArgs.TaskId)
			call("Coordinator.FinishTask", &taskFinishArgs, &taskFinishReply)

		} else if applyTaskReply.TaskType == TaskTypeReduce {
			taskId := applyTaskReply.TaskId
			taskTerm := applyTaskReply.TaskTerm
			mapCnt := applyTaskReply.MapTaskCnt

			// 读取临时文件，组成kv-list
			intermidateKVList := make([]KeyValue, 0)
			for i := 0; i < mapCnt; i++ {
				ifilename := fmt.Sprintf("mr-%d-%d", i, taskId)
				ifile, err := os.Open(ifilename)
				if err != nil {
					fmt.Println(err)
					return
				}
				dec := json.NewDecoder(ifile)
				for dec.More() {
					var kv KeyValue
					if err = dec.Decode(&kv); err != nil {
						fmt.Println(err)
						continue
					}
					intermidateKVList = append(intermidateKVList, kv)
				}
				ifile.Close()
			}

			// 排序
			sort.Sort(KeyValueList(intermidateKVList))

			// 结果文件
			ofilename := fmt.Sprintf("mr-out-%d", taskId)
			ofile, err := os.CreateTemp("", ofilename+"*")
			if err != nil {
				fmt.Println(err)
				return
			}

			for i := 0; i < len(intermidateKVList); {
				j := i
				for j < len(intermidateKVList) && intermidateKVList[i].Key == intermidateKVList[j].Key {
					j++
				}
				key := intermidateKVList[i].Key
				valList := make([]string, 0, j-i)
				for i < j {
					valList = append(valList, intermidateKVList[i].Value)
					i++
				}
				fmt.Fprintf(ofile, "%s %s\n", key, reducef(key, valList))
			}
			os.Rename(ofile.Name(), ofilename)
			ofile.Close()

			// 删除临时文件
			// for i := 0; i < mapCnt; i++ {
			// 	ifilename := fmt.Sprintf("mr-%d-%d", i, taskId)
			// 	err := os.Remove(ifilename)
			// 	if err != nil {
			// 		fmt.Println(err)
			// 		continue
			// 	}
			// }

			// 通知master任务完成
			taskFinishArgs := &FinishTaskArgs{TaskType: TaskTypeReduce, TaskId: taskId, TaskTerm: taskTerm}
			taskFinishReply := &FinishTaskReply{}
			mylog.Infof("[worker %d]FinishReduceTask request, taskId: %d", os.Getpid(), taskFinishArgs.TaskId)
			call("Coordinator.FinishTask", &taskFinishArgs, &taskFinishReply)
		}
		time.Sleep(time.Second)
	}
	mylog.Infof("[worker %d]Done", os.Getpid())
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
