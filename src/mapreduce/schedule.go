package mapreduce

import "fmt"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO

	var reply struct{ OK bool }
	switch phase {
	case mapPhase:
		mapChan := make(chan int, ntasks)
		for i := 0; i < ntasks; i++ {
			go func(index int) {
				for {
					workerAddress := <-mr.registerChannel
					fmt.Printf("pull from register channel, wordAddress %v\n", workerAddress)
					arg := &DoTaskArgs{}
					arg.JobName = mr.jobName
					arg.File = mr.files[index]
					arg.Phase = mapPhase
					arg.TaskNumber = index
					arg.NumOtherPhase = nios
					ok := call(workerAddress, "Worker.DoTask", arg, &reply)

					args := new(RegisterArgs)
					args.Worker = workerAddress
					call(mr.address, "Master.Register", args, new(struct{}))

					if ok == false {
						fmt.Printf("mapPhase error RPC %s register error\n", workerAddress)
					} else {
						mapChan <- index
						return
					}
				}
			}(i)
		}
		for i := 0; i < ntasks; i++ {
			<-mapChan
		}
		fmt.Printf("Schedule: %v phase done\n", phase)
	case reducePhase:
		reduceChan := make(chan int, ntasks)
		for i := 0; i < ntasks; i++ {
			go func(index int) {
				for {
					workerAddress := <-mr.registerChannel
					arg := &DoTaskArgs{}
					arg.JobName = mr.jobName
					arg.Phase = reducePhase
					arg.TaskNumber = index
					arg.NumOtherPhase = nios
					ok := call(workerAddress, "Worker.DoTask", arg, &reply)

					args := new(RegisterArgs)
					args.Worker = workerAddress
					call(mr.address, "Master.Register", args, new(struct{}))

					if ok == false {
						fmt.Printf("reducePhase error RPC %s register error\n", workerAddress)
					} else {
						reduceChan <- index
						return
					}
				}
			}(i)
		}
		for i := 0; i < ntasks; i++ {
			<-reduceChan
		}
		fmt.Printf("Schedule: %v phase done\n", phase)
	}
	return
}
