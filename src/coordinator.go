package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "errors"
import "sync"
import "time"

type Coordinator struct {
	WorkerStates        map[int]int // map Worker Id to Task Id, with 0 ==> Jobless
	Tasks               []Task      // all tasks that need to happen
	Amt_Workers         int         // how many workers we have so far; used to give workers Ids
	nReduce             int         // How many reduce tasks to use
	TaskMutex           sync.Mutex  // For locking shared access to Tasks slice
	Amt_Completed_Tasks int         // How many tasks have been completed so far?
	Reduce_Flag         bool        // Have we started reducing yet?
}

func (c *Coordinator) GetTask(req *TaskRequest, reply *TaskResponse) error {
	log.Printf("Received task request from worker id %v\n", req.WorkerId)
	if req.WorkerId == 0 {
		c.Amt_Workers += 1
		reply.WorkerId = c.Amt_Workers
	} else {
		reply.WorkerId = req.WorkerId
	}
	c.TaskMutex.Lock()
	defer c.TaskMutex.Unlock()
	for i, t := range c.Tasks {
		if t.IsReady() {
			c.Tasks[i].State = In_Progress
			c.Tasks[i].StartTime = time.Now()
			reply.TaskId = t.Id
			reply.TaskType = t.Type
			reply.MapTask = t.MapTask
			reply.ReduceTask = t.ReduceTask
			reply.NReduce = c.nReduce
			c.WorkerStates[reply.WorkerId] = reply.TaskId
			return nil
		}
	}
	return errors.New("No tasks available")
}

func (c *Coordinator) CreateReduceTasks() {
	// take all Int Files from each map task and put them into nReduce amt of Reduce tasks
	intermediate_outfiles := make([][]string, c.nReduce)
	for _, t := range c.Tasks {
		if t.Type != Map { // This should always be the case, but just making sure
			continue
		}
		for reduce_task_num, value := range t.MapTask.OutFiles {
			// t.MapTask.OutFiles is map of {REDUCE_TASK_NUMBER: OUTFILE_NAME}
			intermediate_outfiles[reduce_task_num] = append(intermediate_outfiles[reduce_task_num], value)
		}
	}
	for i, io := range intermediate_outfiles {
		rt := Reduce_Task{}
		rt.InFiles = io
		rt.OutFile = fmt.Sprintf("mr-out-%v", i)

		t := Task{}
		t.Id = len(c.Tasks) + 1
		t.Type = Reduce
		t.State = Idle
		t.ReduceTask = rt
		c.Tasks = append(c.Tasks, t)
	}
	c.Reduce_Flag = true
}

func (c *Coordinator) MarkTaskComplete(tc *TaskComplete, tr *TaskRequest) error {
	log.Printf("Received task complete response from worker id %v\n", tc.WorkerId)
	c.TaskMutex.Lock()
	defer c.TaskMutex.Unlock()
	for i, t := range c.Tasks {
		if t.Id != tc.TaskId {
			continue
		}
		if t.State == Completed {
			return nil
		}
		c.WorkerStates[tc.WorkerId] = 0
		c.Tasks[i].State = Completed
		c.Amt_Completed_Tasks += 1

		if tc.TaskType == Map {
			c.Tasks[i].MapTask.OutFiles = tc.Int_Outfiles
			if c.Amt_Completed_Tasks == len(c.Tasks) {
				go c.CreateReduceTasks()
			}
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	log.Printf("Starting server ...\n")
	rpc.Register(c)
	rpc.HandleHTTP()
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
	return c.Reduce_Flag && len(c.Tasks) == c.Amt_Completed_Tasks
}

func (c *Coordinator) init_tasks() {
	doc_names := os.Args[1:]
	for _, v := range doc_names {

		mapTask := Map_Task{}
		mapTask.InFile = v
		mapTask.OutFiles = make(map[int]string)

		task := Task{}
		task.Id = len(c.Tasks) + 1
		task.Type = Map
		task.State = Idle
		task.MapTask = mapTask

		c.Tasks = append(c.Tasks, task)
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	f, err := os.OpenFile("out.log", os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	log.SetOutput(f)
	c := Coordinator{}

	c.WorkerStates = make(map[int]int)
	c.Tasks = []Task{}
	c.init_tasks()
	c.Amt_Workers = 0
	c.nReduce = nReduce
	c.Amt_Completed_Tasks = 0
	log.Printf("%v is nreduce\n", c.nReduce)

	c.server()
	return &c
}
