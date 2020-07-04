package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	taskCh    chan Task
	files     []string
	nReduce   int
	taskPhase TaskPhase
	taskStats []TaskStat
	workerID  int
	mu        sync.Mutex
	done      bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	fmt.Println("rpc regist......")
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	fmt.Println("listen..........")
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.done
}

func (m *Master) initMapTasks() {
	m.taskPhase = MapPhase
	m.taskStats = make([]TaskStat, len(m.files))
	fmt.Println("Map task init...")
}

func (m *Master) initReduceTasks() {
	m.taskPhase = ReducePhase
	m.taskStats = make([]TaskStat, m.nReduce)
}

func (m *Master) addTask(taskID int) {
	m.taskStats[taskID].Status = TaskStatusQueue
	task := Task{
		FileName: "",
		NReduce:  m.nReduce,
		NMaps:    len(m.files),
		TaskID:   taskID,
		Phase:    m.taskPhase,
		Alive:    true,
	}
	if m.taskPhase == MapPhase {
		task.FileName = m.files[taskID]
	}
	m.taskCh <- task
}

func (m *Master) regTask(args *ReqTaskArgs, task *Task) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// ts := m.taskStats[task.TaskID]
	m.taskStats[task.TaskID].Status = TaskStatusRunning
	m.taskStats[task.TaskID].StartTime = time.Now()
	m.taskStats[task.TaskID].WorkerID = args.WorkerID
}

func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// ts := m.taskStats[args.Seq]
	if args.Done {
		m.taskStats[args.Seq].Status = TaskStatusFinished
	} else {
		m.taskStats[args.Seq].Status = TaskStatusErr
	}
	go m.schedule()
	return nil
}

func (m *Master) checkBreak(taskID int) {
	timeGap := time.Now().Sub(m.taskStats[taskID].StartTime)
	if timeGap > MaxTaskRunTime {
		m.addTask(taskID)
	}
}

func (m *Master) schedule() {
	fmt.Println("in loop.............")
	allFinish := true
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.done {
		return
	}
	for seq, ts := range m.taskStats {
		fmt.Println(seq, " in...", ts.Status)
		switch ts.Status {
		case TaskStatusReady:
			allFinish = false
			m.addTask(seq)
		case TaskStatusQueue:
			allFinish = false
		case TaskStatusRunning:
			allFinish = false
			m.checkBreak(seq)
		case TaskStatusFinished:
		case TaskStatusErr:
			allFinish = false
			m.addTask(seq)
		default:
			panic("tasks status schedule error...")
		}
	}
	if allFinish {
		if m.taskPhase == MapPhase {
			m.initReduceTasks()
		} else {
			m.done = true
		}
	}
}

func (m *Master) tickSchedule() {
	for !m.Done() {
		fmt.Println("loop....")
		go m.schedule()
		time.Sleep(ScheduleInterval)
	}
}

//exposed func to worker here!
func (m *Master) RegWorker(args *RegisterArgs, reply *RegisterReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	fmt.Println("register worker........")
	m.workerID += 1
	reply.WorkerID = m.workerID
	return nil
}

func (m *Master) ReqTask(args *ReqTaskArgs, reply *ReqTaskReply) error {
	task := <-m.taskCh
	reply.Task = &task
	fmt.Println("request task...", task.Alive)
	if task.Alive {
		m.regTask(args, &task)
	}
	fmt.Println("in get one Task, args:%+v, reply:%+v", args, reply)
	return nil
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	// Your code here.
	m.mu = sync.Mutex{}
	m.nReduce = nReduce
	m.files = files
	if nReduce > len(files) {
		m.taskCh = make(chan Task, nReduce)
	} else {
		m.taskCh = make(chan Task, len(m.files))
	}
	m.initMapTasks()
	go m.tickSchedule()

	m.server()
	fmt.Println("master init...")
	return &m
}
