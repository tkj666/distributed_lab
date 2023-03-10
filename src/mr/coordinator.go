package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)


type Coordinator struct {
	// Your definitions here.
	mapJob []KeyValue
	mapQueue queue
	reduceJob [][]KeyValue
	reduceJobMutex []sync.Mutex
	reduceQueue queue
	nReduce int
}
type queue struct {
	job chan int
	done []bool
	doneOnce []sync.Once
	doneCount int32
	size int
	allDone bool
}

const timeout = 10 * time.Second

// Your code here -- RPC handlers for the worker to call.

func makeQueue(size int) queue {
	return queue{
		job: make(chan int, size),
		done: make([]bool, size),
		doneOnce: make([]sync.Once, size),
		doneCount: 0,
		size: size,
		allDone: false,
	}
}
func (q *queue) finish(id int, work func()) {
	q.doneOnce[id].Do(func() {
		q.done[id] = true
		work()
		if atomic.AddInt32(&q.doneCount, 1) == int32(q.size) {
			close(q.job)
			q.allDone = true
		}
	})
}
func (q *queue) timeout(id int) {
	time.Sleep(timeout)
	if !q.done[id] {
		// fmt.Println(id)
		q.job <- id
	}
}
func (c *Coordinator) JobDistro(args *Args, reply *Job) error {
	a, ok := <-c.mapQueue.job
	if ok { // map
		reply.Type = 0
		reply.Val = make([]KeyValue, 1)
		reply.Val[0] = c.mapJob[a]
		reply.Id = a
		// fmt.Println(reply.Type, reply.Id, len(reply.Val))
		go c.mapQueue.timeout(a)
		return nil
	}
	a, ok = <-c.reduceQueue.job
	if ok { // reduce
		reply.Type = 1
		reply.Val = make([]KeyValue, len(c.reduceJob[a]))
		copy(reply.Val, c.reduceJob[a])
		reply.Id = a
		go c.reduceQueue.timeout(a)
	} else { // done
		reply.Type = 2
	}
	return nil
}
func (c *Coordinator) MapSubmit(payload *Payload, _ *struct{}) error {
	id := payload.Id
	c.mapQueue.finish(id, func() {
		for _, val := range payload.Val {
			rid := ihash(val.Key) % c.nReduce
			c.reduceJobMutex[rid].Lock()
			c.reduceJob[rid] = append(c.reduceJob[rid], val)
			c.reduceJobMutex[rid].Unlock()
		}
		// fmt.Println(len(payload.Val))
	})
	// fmt.Println("e")
	return nil
}
func (c *Coordinator) ReduceSubmit(payload *Payload, _ *struct{}) error {
	id := payload.Id
	c.reduceQueue.finish(id, func() {
		oname := fmt.Sprintf("mr-out-%d", id)
		ofile, _ := os.Create(oname)
		for _, val := range payload.Val {
			fmt.Fprintf(ofile, "%v %v\n", val.Key, val.Value)
		}
		ofile.Close()
	})
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	return c.reduceQueue.allDone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	size := len(files)
	c := Coordinator{
		mapJob: make([]KeyValue, size),
		mapQueue: makeQueue(size),
		reduceJob: make([][]KeyValue, nReduce),
		reduceJobMutex: make([]sync.Mutex, nReduce),
		reduceQueue: makeQueue(nReduce),
		nReduce: nReduce,
	}

	// Your code here.
	for i, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		c.mapJob[i] = KeyValue{filename, string(content)}
		c.mapQueue.job <- i
	}
	for i := 0; i < nReduce; i++ {
		c.reduceQueue.job <- i
		c.reduceJob[i] = make([]KeyValue, 0)
	}

	c.server()
	return &c
}
