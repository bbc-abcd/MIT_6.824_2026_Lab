package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

// Task kinds carried in GetTaskReply.Kind.
const (
	MapTask    = iota // 0
	ReduceTask        // 1
	WaitTask          // 2
	ExitTask          // 3
)

// GetTaskArgs is empty: a worker just asks for its next task.
type GetTaskArgs struct{}

// GetTaskReply describes the task a worker should run.
type GetTaskReply struct {
	Kind     int      // MapTask / ReduceTask / WaitTask / ExitTask
	MapFile  string   // input file (MapTask)
	ReduceID int      // reduce partition (ReduceTask)
	NReduce  int      // number of reduce partitions
	Files    []string // intermediate files to merge (ReduceTask)
}

// ReportMapArgs is sent after a worker finishes a map task.
type ReportMapArgs struct {
	MapFile string   // input file that was processed
	Files   []string // 下标为 reduceID，值为该分区产出的中间文件
}

// ReportReduceArgs is sent after a worker finishes a reduce task.
type ReportReduceArgs struct {
	ReduceID int
}

// ReportReply is empty.
type ReportReply struct{}
