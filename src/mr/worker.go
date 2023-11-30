package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
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
		call("Coordinator.ApplyTask", &applyTaskArgs, &applyTaskReply)

		if applyTaskReply.TaskType == TaskTypeMap {
			filename := applyTaskReply.Filename
			taskId := applyTaskReply.TaskId
			reduceCnt := applyTaskReply.ReduceTaskCnt

			// 读取file
			fileContent, err := os.ReadFile(filename)
			if err != nil {
				fmt.Println(err)
				break
			}

			// mapf解析
			kvList := mapf(filename, string(fileContent))

			// 创建临时文件
			intermidateFile := make([]*os.File, reduceCnt)
			for i := 0; i < reduceCnt; i++ {
				ofilename := fmt.Sprintf("mr-%d-%d", taskId, i)
				ofile, err := os.Create(ofilename)
				if err != nil {
					fmt.Println(err)
					break
				}
				intermidateFile[i] = ofile
			}

			// 输出到reduceCnt个临时文件，每个key根据hash映射到文件
			for _, kv := range kvList {
				reduceId := ihash(kv.Key) % reduceCnt
				ofile := intermidateFile[reduceId]
				kvByte, err := json.Marshal(kv)
				if err != nil {
					fmt.Println(err)
					continue
				}
				kvByte = append(kvByte, '\n')
				ofile.Write(kvByte)
			}

			// 关闭临时文件
			for i := 0; i < reduceCnt; i++ {
				intermidateFile[i].Close()
			}

			// 通知master任务完成
			taskFinishArgs := &FinishTaskArgs{TaskType: TaskTypeMap, TaskId: taskId}
			taskFinishReply := &FinishTaskReply{}
			call("Coordinator.FinishTask", &taskFinishArgs, &taskFinishReply)

		} else if applyTaskReply.TaskType == TaskTypeReduce {
			taskId := applyTaskReply.TaskId
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
			ofile, err := os.Create(ofilename)
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

			ofile.Close()

			// 删除临时文件
			for i := 0; i < mapCnt; i++ {
				ifilename := fmt.Sprintf("mr-%d-%d", i, taskId)
				err := os.Remove(ifilename)
				if err != nil {
					fmt.Println(err)
					continue
				}
			}

			// 通知master任务完成
			taskFinishArgs := &FinishTaskArgs{TaskType: TaskTypeReduce, TaskId: taskId}
			taskFinishReply := &FinishTaskReply{}
			call("Coordinator.FinishTask", &taskFinishArgs, &taskFinishReply)

		} else {
			// fmt.Println("no task left!")
			break
		}
	}

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
