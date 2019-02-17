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

	var waitGroup sync.WaitGroup // adding waitGroup for a worker

	for task := 0; task < ntasks; task++ {
		doTask := DoTaskArgs{jobName, mapFiles[task], phase, task, n_other} // getting the RPC arguments
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			serverResponseSuccess := false

			for !serverResponseSuccess {
				workerAddress := <-registerChan
				serverResponseSuccess := call(workerAddress, "Worker.DoTask", doTask, nil) //sending RPC to a worker

				if serverResponseSuccess { //waiting for the worker to finish task as call() method returns
					//false in case of any failure or time out
					go func() { registerChan <- workerAddress }()
					break
				}
			}

		}()
	}
	waitGroup.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}
