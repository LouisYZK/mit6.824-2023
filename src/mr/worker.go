package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strings"
)

type worker struct {
	id      int
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	w := worker{}
	w.mapf = mapf
	w.reducef = reducef

	w.register()
	w.run()
	// uncomment to send the Example RPC to the master.

}

func (w *worker) run() {
	for {
		t := w.reqTask()
		if !t.Alive {
			fmt.Println("worker get task not alive, worker %d exit..", w.id)
			return
		}
		w.doTask(t)
	}
}

func (w *worker) register() {
	args := &RegisterArgs{}
	reply := &RegisterReply{}
	if ok := call("Master.RegWorker", args, reply); !ok {
		log.Fatal("register error...")
	}
	w.id = reply.WorkerID
	fmt.Println("worker registed...", w.id)
}

func (w *worker) reqTask() Task {
	args := ReqTaskArgs{}
	args.WorkerID = w.id
	reply := ReqTaskReply{}
	if ok := call("Master.ReqTask", &args, &reply); !ok {
		log.Fatal("request for task fail...")
	}
	return *reply.Task
}

func (w *worker) reportTask(task Task, done bool, err error) {
	if err != nil {
		log.Printf("%v", err)
	}
	args := ReportTaskArgs{}
	args.Done = done
	args.Seq = task.TaskID
	args.Phase = task.Phase
	args.WorkerId = w.id
	reply := ReportTaskReply{}
	if ok := call("Master.ReportTask", &args, &reply); !ok {
		fmt.Println("report task fail:%+v", args)
	}
}

func (w *worker) doTask(task Task) {
	if task.Phase == MapPhase {
		w.doMapTask(task)
	} else if task.Phase == ReducePhase {
		w.doReduceTask(task)
	} else {
		panic(fmt.Sprintf("task phase err: %v", task.Phase))
	}
}

func (w *worker) doMapTask(task Task) {
	content, err := ioutil.ReadFile(task.FileName)
	if err != nil {
		log.Fatalf("cannot read %v", task.FileName)
		w.reportTask(task, false, err)
	}
	kvs := w.mapf(task.FileName, string(content))
	reduces := make([][]KeyValue, task.NReduce)
	for _, kv := range kvs {
		idx := ihash(kv.Key) % task.NReduce
		reduces[idx] = append(reduces[idx], kv)
	}
	for rno, l := range reduces {
		fileName := fmt.Sprint("mr-%d-%d", task.TaskID, rno)
		f, err := os.Create(fileName)
		if err != nil {
			w.reportTask(task, false, err)
		}
		enc := json.NewEncoder(f)
		for _, kv := range l {
			if err := enc.Encode(kv); err != nil {
				w.reportTask(task, false, err)
			}
		}
		if err := f.Close(); err != nil {
			w.reportTask(task, false, err)
		}
	}
	w.reportTask(task, true, nil)
}

func (w *worker) doReduceTask(task Task) {
	maps := make(map[string][]string)
	for idx := 0; idx < task.NMaps; idx++ {
		fileName := fmt.Sprint("mr-%d-%d", idx, task.TaskID)
		file, err := os.Open(fileName)
		if err != nil {
			w.reportTask(task, false, err)
			return
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0, 100)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}
	res := make([]string, 0, 100)
	for k, v := range maps {
		res = append(res, fmt.Sprintf("%v %v\n", k, w.reducef(k, v)))
	}
	err := ioutil.WriteFile(fmt.Sprintf("mr-out-%d", task.TaskID), []byte(strings.Join(res, "")), 0600)
	if err != nil {
		w.reportTask(task, false, err)
		return
	}
	w.reportTask(task, true, nil)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
