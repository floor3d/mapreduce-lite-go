package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type WorkerIdentity struct {
	WorkerId int
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.

// func Map(filename string, contents string) []mr.KeyValue
func Run_Map(t *TaskResponse, mapf func(string, string) []KeyValue) map[int]string {
	filename := t.MapTask.InFile
	contents, err := os.ReadFile(filename)
	if err != nil {
		panic(err)
	}

	kv_result := mapf(filename, string(contents))
	buffered_outfiles := make([]string, t.NReduce)
	for _, v := range kv_result {
		file_number := ihash(v.Key) % t.NReduce
		buffered_outfiles[file_number] += fmt.Sprintf("%v %v,", v.Key, v.Value)

	}
	int_filenames := make(map[int]string)
	for i, v := range buffered_outfiles {
		if len(v) <= 1 {
			continue
		}
		// Remove the last "," from the slice
		buffered_outfiles[i] = buffered_outfiles[i][:len(v)-1]

		outfilename := fmt.Sprintf("mr-int-%v-%v-%v", t.WorkerId, t.TaskId, i)
		int_filenames[i] = outfilename
		f, err := os.Create(outfilename)
		if err != nil {
			panic(err)
		}
		defer f.Close()

		n, err := f.WriteString(buffered_outfiles[i])
		log.Printf("Writing buffered outfile with length %v to output file %v\n", n, outfilename)
		if err != nil {
			panic(err)
		}
		f.Sync()
	}
	return int_filenames
}

func grab_kvs(t *TaskResponse) map[string][]string {
	all_kvs := make(map[string][]string)
	for _, fname := range t.ReduceTask.InFiles {
		contents, err := os.ReadFile(fname)
		if err != nil {
			panic(err)
		}
		raw := strings.Split(string(contents), ",")

		for _, v := range raw {
			v1v2 := strings.Split(v, " ")
			_, exists := all_kvs[v1v2[0]]
			if !exists {
				all_kvs[v1v2[0]] = []string{}
			}
			all_kvs[v1v2[0]] = append(all_kvs[v1v2[0]], v1v2[1])
		}
	}
	return all_kvs
}

// func Reduce(key string, values []string) string
func Run_Reduce(t *TaskResponse, reducef func(string, []string) string) {
	current_dir, _ := os.Getwd()
	f, err := os.CreateTemp(current_dir, "tempout")
	if err != nil {
		panic(err)
	}
	log.Printf("Reduce task outfile is %v\n", t.ReduceTask.OutFile)
	defer f.Close()
	all_kvs := grab_kvs(t)

	for key, arr := range all_kvs {
		output := reducef(key, arr)
		fmt.Fprintf(f, "%v %v\n", key, output)
	}
	err = os.Rename(f.Name(), t.ReduceTask.OutFile)
	if err != nil {
		os.Remove(f.Name())
	}
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	f, err := os.OpenFile("out.log", os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	log.SetOutput(f)

	w := WorkerIdentity{0}
	log.Println("Worker initialized")

	for {
		log.Println("Asking for task...")
		reply := TaskResponse{}
		ok := RequestTask(&w, &reply)
		log.Printf("Task response: %v\n", reply)
		if !ok {
			time.Sleep(time.Second * 1)
			continue
		}
		w.WorkerId = reply.WorkerId
		tc := TaskComplete{}
		tc.WorkerId = w.WorkerId
		tc.TaskId = reply.TaskId
		tc.TaskType = reply.TaskType
		if reply.TaskType == Map {
			int_outfiles := Run_Map(&reply, mapf)
			tc.Int_Outfiles = int_outfiles
		} else {
			Run_Reduce(&reply, reducef)
		}
		CompleteTask(&tc)
	}

}

func CompleteTask(tc *TaskComplete) {
	ok := call("Coordinator.MarkTaskComplete", tc, nil)
	if !ok {
		log.Println("Complete task failed?")
	}
}

func RequestTask(w *WorkerIdentity, reply *TaskResponse) bool {
	args := TaskRequest{w.WorkerId}
	ok := call("Coordinator.GetTask", &args, reply)
	return ok
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args any, reply any) bool {
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

	log.Println(err)
	return false
}
