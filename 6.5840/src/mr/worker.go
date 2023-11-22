package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		response := doHeartbeat()
		log.Printf("Worker: receive coordinator's heartbeat %v \n", response)
		switch response.JobType {
		case MapJob:
			doMapTask(mapf, response)
			return
		case ReduceJob:
			fmt.Print("get a reduceJob\n")
		case WaitJob:
			fmt.Print("get a waitJob\n")
		case CompleteJob:
			return
		default:
			panic(fmt.Sprintf("unexpected jobType %v", response.JobType))
		}
	}

}

func doMapTask(mapF func(string, string) []KeyValue, response *HeartbeatResponse) {
	wordCountList := getWordCountListOfFile(mapF, response.FilePath)
	// fmt.Printf("wordCountList: %v\n", wordCountList)

	intermediate := splitWordCountListToReduceNNum(wordCountList, response.NReduce)
	// fmt.Printf("intermediate: %v\n", intermediate)

	var writeIntermediateFilewg sync.WaitGroup
	for reduceNumber, splitedWordCountList := range intermediate {
		writeIntermediateFilewg.Add(1)
		go func(reduceNumber int, splitedWordCountList []KeyValue) {
			defer writeIntermediateFilewg.Done()
			writeIntermediateFile(response.Id, reduceNumber, splitedWordCountList)
		}(reduceNumber, splitedWordCountList)
	}
	writeIntermediateFilewg.Wait()

}

func doReport(id int, phase SchedulePhase) {
	call("Coordinator.Report", &ReportRequest{Id: id, Phase: phase}, &ReportResponse{})
}

func getWordCountListOfFile(mapF func(string, string) []KeyValue, filePath string) []KeyValue {
	file, err := os.Open(filePath)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", filePath)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filePath)
	}
	return mapF(filePath, string(content))
}

func splitWordCountListToReduceNNum(wordCountList []KeyValue, nReduce int) [][]KeyValue {
	intermediate := make([][]KeyValue, nReduce)
	for _, wordCount := range wordCountList {
		word := wordCount.Key
		reduceNumber := ihash(word) % nReduce
		intermediate[reduceNumber] = append(intermediate[reduceNumber], wordCount)
	}
	return intermediate
}

func writeIntermediateFile(mapNumber int, reduceNumber int, wordCountList []KeyValue) {
	fileName := generateMapResultFileName(mapNumber, reduceNumber)
	file, err := os.Create(fileName)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot create %v", fileName)
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, wordCount := range wordCountList {
		err := enc.Encode(&wordCount)
		if err != nil {
			log.Fatalf("cannot encode %v", wordCount)
		}
	}
	atomicWriteFile(fileName, &buf)
}

func atomicWriteFile(filename string, reader io.Reader) (err error) {
	tmpFileName, err := writeToTmpFile(filename, reader)
	if err != nil {
		return fmt.Errorf("cannot write to temp file: %v", err)
	}

	if err := os.Rename(tmpFileName, filename); err != nil {
		return fmt.Errorf("cannot rename temp file: %v", err)
	}

	return nil
}

func writeToTmpFile(filename string, reader io.Reader) (tmpFileName string, err error) {
	dir, file := filepath.Split(filename)
	if dir == "" {
		dir = "."
	}

	tmpFile, err := os.CreateTemp(dir, file)
	if err != nil {
		return "", fmt.Errorf("cannot create temp file: %v", err)
	}
	defer tmpFile.Close()
	defer func() {
		if err != nil {
			os.Remove(tmpFile.Name())
		}
	}()

	_, err = io.Copy(tmpFile, reader)
	if err != nil {
		return "", fmt.Errorf("cannot write to temp file: %v", err)
	}
	if err := tmpFile.Close(); err != nil {
		return "", fmt.Errorf("cannot close temp file: %v", err)
	}

	return tmpFile.Name(), nil
}

func doHeartbeat() *HeartbeatResponse {
	response := HeartbeatResponse{}
	call("Coordinator.Heartbeat", &HeartbeatRequest{}, &response)
	return &response
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
