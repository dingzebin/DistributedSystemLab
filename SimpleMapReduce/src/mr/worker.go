package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
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

const (
	MAP_OUT_PREFIX    = "map-"
	REDUCE_OUT_PREFIX = "mr-out-"
)

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
	for {
		args := GetTaskArgs{}
		task := GetTaskReply{}
		ok := call("Coordinator.GetTask", &args, &task)
		if !ok {
			log.Fatalf("Failed to call the Coordinator.GetTask!")
			break
		}
		if task.TaskType == FINISHED {
			break
		}
		if len(task.Files) == 0 {
			time.Sleep(time.Second)
			continue
		}

		var e error
		if MAP == task.TaskType {
			e = handleMap(mapf, task)
		} else if REDUCE == task.TaskType {
			e = handleReduce(reducef, task)
		}
		if e != nil {
			log.Fatal(e)
			continue
		}
	}
}

// handle map
func handleMap(mapf func(string, string) []KeyValue, task GetTaskReply) error {
	fileName := task.Files[0]
	file, err := os.Open(fileName)
	if err != nil {
		return err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}
	file.Close()

	kva := mapf(fileName, string(content))
	kvas := partition(kva, task.NReduce)
	for i, v := range kvas {
		fileName := fmt.Sprintf(MAP_OUT_PREFIX+"%v-%v", task.No, i)
		file, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModePerm)
		if err != nil {
			return err
		}
		encoder := json.NewEncoder(file)
		err = encoder.Encode(v)
		if err != nil {
			return err
		}
		file.Close()
		args := IntermadiateArgs{ReduceNo: i, File: fileName}
		reply := IntermadiateReply{}
		ok := call("Coordinator.PostIntermadiateFile", &args, &reply)
		if !ok {
			return errors.New("Failed to call Coordinator.PostIntermadiateFile")
		}
	}
	confirm(task)
	return nil
}

// handle the reduce
func handleReduce(reducef func(string, []string) string, task GetTaskReply) error {
	intermediate := []KeyValue{}
	for _, fileName := range task.Files {
		file, err := os.OpenFile(fileName, os.O_RDONLY, os.ModePerm)
		if err != nil {
			return err
		}
		dec := json.NewDecoder(file)
		for {
			var kv []KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv...)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))

	type ReduceOutput struct {
		key    string
		output string
	}
	outputs := []ReduceOutput{}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		outputs = append(outputs, ReduceOutput{key: intermediate[i].Key, output: output})
		i = j
	}
	oname := REDUCE_OUT_PREFIX + strconv.Itoa(task.No)
	_, err := os.Stat(oname)
	if err == nil {
		return nil
	}
	ofile, _ := os.Create(oname)
	for _, v := range outputs {
		fmt.Fprintf(ofile, "%v %v\n", v.key, v.output)
	}
	ofile.Close()
	err = confirm(task)
	if err != nil {
		return err
	}
	for _, fileName := range task.Files {
		os.Remove(fileName)
	}
	return nil
}

func confirm(task GetTaskReply) error {
	args := ConfirmArgs{No: task.No}
	reply := ConfirmReply{}
	ok := call("Coordinator.Confirm", &args, &reply)
	if !ok {
		return errors.New("Failed to call the Coordinator.Confirm!")
	}
	return nil
}

func partition(kva []KeyValue, nReduce int) map[int][]KeyValue {
	kvas := map[int][]KeyValue{}
	for _, v := range kva {
		i := ihash(v.Key) % nReduce
		if _, ok := kvas[i]; !ok {
			kvas[i] = []KeyValue{}
		}
		kvas[i] = append(kvas[i], v)
	}
	return kvas
}

//
// example function to show how to make an RPC call to the coordinator.
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
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
	//	fmt.Println("sockname =>", sockname)
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
