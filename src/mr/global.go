package mr

import "time"

type TaskPhase int
type TaskStatus int

const (
	MapPhase    TaskPhase = 1
	ReducePhase TaskPhase = 2
)

const (
	TaskStatusReady    TaskStatus = 0
	TaskStatusQueue    TaskStatus = 1
	TaskStatusRunning  TaskStatus = 2
	TaskStatusFinished TaskStatus = 3
	TaskStatusErr      TaskStatus = 4
)

const (
	MaxTaskRunTime   = time.Second * 5
	ScheduleInterval = time.Millisecond * 500
)

type Task struct {
	FileName string
	NReduce  int
	NMaps    int
	TaskID   int
	Phase    TaskPhase
	Alive    bool
}

type TaskStat struct {
	TaskID    int
	Phase     TaskPhase
	Status    TaskStatus
	WorkerID  int
	StartTime time.Time
}
