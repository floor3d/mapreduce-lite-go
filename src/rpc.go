package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"
import "time"

type Task_Type int

const (
	Map    Task_Type = iota // 0
	Reduce                  // 1
)

type WorkState int

const (
	Idle        WorkState = iota // 0
	In_Progress                  // 1
	Completed                    // 2
)

type Map_Task struct {
	InFile   string
	OutFiles map[int]string
}

type Reduce_Task struct {
	InFiles []string
	OutFile string
}

// When workers ask for tasks, Coordinator should give them either
// a Task with Idle, or
// a Task with In_Progress && CurrentTime - StartTime >= 10
type Task struct {
	Id         int
	Type       Task_Type
	State      WorkState
	StartTime  time.Time
	MapTask    Map_Task
	ReduceTask Reduce_Task
}

func (t *Task) IsReady() bool {
	not_started := t.State == Idle
	been_ten_seconds := t.State == In_Progress && time.Since(t.StartTime).Seconds() >= 10

	return not_started || been_ten_seconds
}

type TaskRequest struct {
	WorkerId int
}

type TaskResponse struct {
	WorkerId   int
	TaskId     int
	TaskType   Task_Type
	NReduce    int
	MapTask    Map_Task
	ReduceTask Reduce_Task
}

type TaskComplete struct {
	WorkerId     int
	TaskId       int
	TaskType     Task_Type
	Int_Outfiles map[int]string // Map of Reduce Task # to Filename
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
