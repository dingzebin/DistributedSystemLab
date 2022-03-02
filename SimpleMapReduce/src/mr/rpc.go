package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

const (
	MAP      = 0
	REDUCE   = 1
	FINISHED = -1
)

type GetTaskArgs struct {
}

// task
type GetTaskReply struct {
	Files    []string
	TaskType int
	No       int
	NReduce  int
}

// the args from worker to coordinator means the assigned task is finished
type ConfirmArgs struct {
	No int
}

type ConfirmReply struct {
}

type IntermadiateArgs struct {
	ReduceNo int
	File     string
}

type IntermadiateReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
