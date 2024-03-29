package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	taskChan := make(chan int, ntasks)
	wg := sync.WaitGroup{}
	wg.Add(ntasks)
	// Producer
	go func(){
		for i := 0; i < ntasks; i++ {
			taskChan <- i
		}
	}()

	// Consumer
	go func() {
		for task := range taskChan {
			args := DoTaskArgs{
				JobName:       jobName,
				File:          "",
				Phase:         phase,
				TaskNumber:    task,
				NumOtherPhase: n_other,
			}
			if phase == mapPhase {
				args.File = mapFiles[task]
			}
			// Consume
			go func(task int) {
				worker := <-registerChan
				if done := call(worker, "Worker.DoTask", args, nil); done {
					fmt.Println("Debug - Done task ", phase, task)
					wg.Done()
					registerChan <- worker
				} else {
					fmt.Println("Debug - Fail to execute task, sendback job to job queue")
					taskChan <- task
				}
			}(task)
		}
	}()
	wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}
