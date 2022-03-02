package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskInfo struct {
	no    int
	files []string
	quit  chan int
}

const TIMEOUT = 10

type Coordinator struct {
	mu sync.Mutex
	// Your definitions here.
	intermediateFiles map[int][]string
	processingTasks   map[int]*TaskInfo
	tasks             []TaskInfo
	nReduce           int
	taskType          int
	done              chan bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(taskArgs *GetTaskArgs, taskReply *GetTaskReply) error {
	c.mu.Lock()
	if c.taskType == FINISHED {
		taskReply.TaskType = FINISHED
	} else if len(c.tasks) > 0 {
		t := c.tasks[0]
		taskReply.Files = t.files
		taskReply.TaskType = c.taskType
		taskReply.No = t.no
		taskReply.NReduce = c.nReduce

		// delete task
		c.tasks = c.tasks[1:]

		t.quit = make(chan int)
		// joining the processing tasks
		c.processingTasks[t.no] = &t
		go c.monitor(&t)
	}
	c.mu.Unlock()
	return nil
}

// monitor tasks is timeout
func (c *Coordinator) monitor(t *TaskInfo) {
	timer := time.NewTimer(TIMEOUT * time.Second)
	for {
		select {
		case <-timer.C:
			c.mu.Lock()
			delete(c.processingTasks, t.no)
			c.tasks = append(c.tasks, *t)
			c.mu.Unlock()
			return
		case <-t.quit: // if task is completed
			timer.Stop()
			return
		}
	}
}

// save imtermadiate files
func (c *Coordinator) PostIntermadiateFile(args *IntermadiateArgs, reply *IntermadiateReply) error {
	c.mu.Lock()
	if _, ok := c.intermediateFiles[args.ReduceNo]; !ok {
		c.intermediateFiles[args.ReduceNo] = []string{}
	}
	c.intermediateFiles[args.ReduceNo] = append(c.intermediateFiles[args.ReduceNo], args.File)
	c.mu.Unlock()
	return nil
}

// call by worker when the task is completed
func (c *Coordinator) Confirm(args *ConfirmArgs, reply *ConfirmReply) error {
	c.mu.Lock()
	if _, ok := c.processingTasks[args.No]; ok {
		c.processingTasks[args.No].quit <- 0
		delete(c.processingTasks, args.No)
	}
	if len(c.tasks) == 0 && len(c.processingTasks) == 0 {
		if MAP == c.taskType {
			c.generateReduceTask()
		} else if REDUCE == c.taskType {
			c.taskType = FINISHED
			c.done <- true
		}
	}
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) generateReduceTask() {
	for k, v := range c.intermediateFiles {
		c.tasks = append(c.tasks, TaskInfo{no: k, files: v})
	}
	c.taskType = REDUCE
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	// Your code here.
	return <-c.done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:           nReduce,
		taskType:          MAP,
		done:              make(chan bool),
		processingTasks:   map[int]*TaskInfo{},
		intermediateFiles: map[int][]string{},
	}
	for i, v := range files {
		c.tasks = append(c.tasks, TaskInfo{no: i, files: []string{v}})
	}
	c.server()
	return &c
}
