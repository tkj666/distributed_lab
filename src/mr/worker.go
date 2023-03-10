package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		job := queryJob()
		// fmt.Println(job.Id, job.Type, len(job.Val))
		switch job.Type {
		case 0: // map
			mapSubmit(mapf(job.Val[0].Key, job.Val[0].Value), job.Id)
		case 1: // reduce
			sort.Sort(ByKey(job.Val))
			l, r := 0, 0
			ans := []KeyValue{}
			for l < len(job.Val) {
				values := []string{}
				for r < len(job.Val) && job.Val[l].Key == job.Val[r].Key {
					values = append(values, job.Val[r].Value)
					r++
				}
				ans = append(ans, KeyValue{job.Val[l].Key, reducef(job.Val[l].Key, values)})
				l = r
			}
			reduceSubmit(ans, job.Id)
		case 2: // done
			return
		}
	}

}

func queryJob() Job {
	args := Args{}
	job := Job{}
	ok := call("Coordinator.JobDistro", &args, &job)
	// fmt.Println(job.Type, job.Id, len(job.Val))
	if !ok {
		fmt.Print("call failed!\n")
	}
	return job
}
func mapSubmit(val []KeyValue, id int) {
	payload := Payload{id, val}
	ok := call("Coordinator.MapSubmit", &payload, nil)
	if !ok {
		fmt.Print("map submit failed!\n")
	}
}
func reduceSubmit(val []KeyValue, id int) {
	payload := Payload{id, val}
	ok := call("Coordinator.ReduceSubmit", &payload, nil)
	if !ok {
		fmt.Print("reduce submit failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
