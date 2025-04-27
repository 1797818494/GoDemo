package main

import (
	"container/heap"
	"fmt"
	"sync"
	"time"
)

// Task represents a node in the DAG
type Task struct {
	ID           string
	ExecuteFunc  func() error  // The actual work the task does
	Dependencies []string      // IDs of tasks that must complete before this one
	Dependents   []string      // IDs of tasks that depend on this one
	Duration     time.Duration // Simulated execution time

	// Fields for critical path scheduling
	distanceToSink time.Duration // Longest path sum of durations from this task to any sink node
	priority       int           // Priority for the priority queue (higher distanceToSink -> higher priority)
}

// DAG represents the directed acyclic graph
type DAG struct {
	Tasks map[string]*Task // Modified: Store *Task directly
}

// NewDAG creates a new DAG
func NewDAG() *DAG {
	return &DAG{
		Tasks: make(map[string]*Task),
	}
}

// AddTask adds a task to the DAG
func (d *DAG) AddTask(task *Task) {
	// Modified: Store the *Task pointer directly
	d.Tasks[task.ID] = task
}

// AddDependency adds a dependency edge from taskFrom to taskTo
func (d *DAG) AddDependency(taskFromID, taskToID string) error {
	// Modified: Retrieve *Task directly
	taskFrom, ok := d.Tasks[taskFromID]
	if !ok {
		return fmt.Errorf("task '%s' not found", taskFromID)
	}
	taskTo, ok := d.Tasks[taskToID]
	if !ok {
		return fmt.Errorf("task '%s' not found", taskToID)
	}

	// Check for basic cycle (optional, robust check needs graph traversal)
	for _, dep := range taskFrom.Dependencies {
		if dep == taskToID {
			return fmt.Errorf("cannot add dependency from %s to %s, dependency already exists", taskFromID, taskToID)
		}
	}

	// Modify the Task structs via the pointers
	taskFrom.Dependents = append(taskFrom.Dependents, taskToID)
	taskTo.Dependencies = append(taskTo.Dependencies, taskFromID)
	return nil
}

// calculateDistanceToSink computes the longest path from each task to any sink node
// This is a crucial step for critical path scheduling
func (d *DAG) calculateDistanceToSink() error {
	// Initialize distances - sinks have distance equal to their own duration
	for _, task := range d.Tasks { // Modified: Iterate over *Task directly
		if len(task.Dependents) == 0 { // It's a sink node
			task.distanceToSink = task.Duration
		} else {
			task.distanceToSink = 0 // Will be computed
		}
	}

	// Use a map to track how many predecessors of a task have had their distance calculated
	// This helps process tasks in a reverse topological-like order
	predecessorDistancesCalculated := make(map[string]int)
	q := []*Task{} // Modified: Queue stores *Task

	// Add all tasks with no dependents (sinks) to the initial queue
	for _, task := range d.Tasks { // Modified: Iterate over *Task directly
		if len(task.Dependents) == 0 {
			q = append(q, task)
		}
	}

	// Perform reverse traversal
	for len(q) > 0 {
		currTask := q[0] // Modified: Get *Task directly from queue
		q = q[1:]

		// Update distance for predecessors
		for _, predID := range currTask.Dependencies {
			// Modified: Retrieve *Task directly
			predTask, ok := d.Tasks[predID]
			if !ok {
				return fmt.Errorf("predecessor task '%s' not found", predID)
			}

			// Distance from predecessor is its duration + distance from current task
			// We want the maximum over all dependents
			potentialDistance := predTask.Duration + currTask.distanceToSink

			if potentialDistance > predTask.distanceToSink {
				predTask.distanceToSink = potentialDistance
			}

			// Increment count for this predecessor
			predecessorDistancesCalculated[predID]++

			// If all dependents of this predecessor have had their distances calculated,
			// add the predecessor to the queue
			if predecessorDistancesCalculated[predID] == len(predTask.Dependents) {
				q = append(q, predTask) // Modified: Add *Task to queue
			}
		}
	}

	// After calculation, set priority based on distanceToSink
	// Higher distance means higher priority (lower value in a min-heap)
	// For a max-heap behavior on distance, priority = distance
	for _, task := range d.Tasks { // Modified: Iterate over *Task directly
		task.priority = int(task.distanceToSink.Milliseconds()) // Use milliseconds for int priority
	}

	return nil
}

// --- Priority Queue Implementation (No change needed here as it stores *Task) ---
// An Item in the priority queue
type PriorityQueueItem struct {
	task     *Task
	priority int // The priority of the item in the queue
	index    int // The index of the item in the heap array
}

// A PriorityQueue implements heap.Interface and holds PriorityQueueItems.
type PriorityQueue []*PriorityQueueItem

func (pq PriorityQueue) Len() int { return len(pq) }

// We want a Max-Heap based on priority (distanceToSink)
func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].priority > pq[j].priority // Higher priority value means 'less' for Max-Heap
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x any) {
	n := len(*pq)
	item := x.(*PriorityQueueItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// --- Scheduler Implementation ---

// Scheduler manages the execution of the DAG
type Scheduler struct {
	dag                *DAG
	readyQueue         *PriorityQueue // Priority queue for tasks that are ready to run
	readySignal        chan struct{}  // Signal channel to notify workers when tasks are added to PQ
	completedTasks     chan *Task     // Channel for completed tasks
	dependencyCounts   map[string]int // How many dependencies each task is waiting for
	runningTasks       sync.Map       // Keep track of tasks currently being executed (ID -> bool)
	workerCount        int
	wg                 sync.WaitGroup // Wait group to wait for all workers to complete
	mu                 sync.Mutex     // Mutex for protecting readyQueue and dependencyCounts
	totalTaskCount     int
	completedTaskCount int
}

// NewScheduler creates a new scheduler with a worker pool of specified size
func NewScheduler(d *DAG, workerCount int) *Scheduler {
	return &Scheduler{
		dag:              d,
		readyQueue:       &PriorityQueue{},
		readySignal:      make(chan struct{}, workerCount), // Buffered signal channel
		completedTasks:   make(chan *Task, len(d.Tasks)),   // Buffered channel
		dependencyCounts: make(map[string]int),
		workerCount:      workerCount,
		totalTaskCount:   len(d.Tasks),
	}
}

// Run executes the DAG
func (s *Scheduler) Run() error {
	if s.dag == nil || s.totalTaskCount == 0 {
		return fmt.Errorf("DAG is empty")
	}

	// 1. Calculate critical path distances (priorities)
	fmt.Println("Calculating critical path distances...")
	err := s.dag.calculateDistanceToSink()
	if err != nil {
		return fmt.Errorf("error calculating critical path distances: %v", err)
	}
	fmt.Println("Critical path distances calculated.")
	// Optional: Print calculated distances for verification
	// for id, task := range s.dag.Tasks { // Modified: Iterate over *Task directly
	//     fmt.Printf("Task %s: Duration=%s, DistanceToSink=%s, Priority=%d\n",
	//         id, task.Duration, task.distanceToSink, task.priority)
	// }

	// 2. Initialize dependency counts and find initial ready tasks
	s.mu.Lock()             // Protect dependencyCounts and readyQueue during initialization
	heap.Init(s.readyQueue) // Initialize the priority queue

	initialReadyCount := 0
	for id, task := range s.dag.Tasks { // Modified: Iterate over *Task directly
		s.dependencyCounts[id] = len(task.Dependencies)
		if len(task.Dependencies) == 0 {
			// Task is initially ready, add to priority queue
			heap.Push(s.readyQueue, &PriorityQueueItem{task: task, priority: task.priority})
			initialReadyCount++
		}
	}
	s.mu.Unlock()

	if initialReadyCount == 0 && s.totalTaskCount > 0 {
		return fmt.Errorf("DAG has no starting tasks (possible cycle?)")
	}
	go func(initialReadyCount int) {
		for i := 0; i < initialReadyCount; i++ {

			s.readySignal <- struct{}{}
		}
	}(initialReadyCount)
	// Send initial signals to wake up workers

	// 3. Start worker goroutines
	s.wg.Add(s.workerCount) // Add for each worker
	for i := 0; i < s.workerCount; i++ {
		go s.worker(i)
	}

	// 4. Monitor completed tasks and update dependencies
	// Use a goroutine to handle completed tasks to avoid blocking workers
	go s.completionMonitor()

	// 5. Wait for all workers to finish (they exit when readySignal channel is closed)
	s.wg.Wait()

	fmt.Println("DAG execution finished.")
	return nil
}

// worker goroutine
func (s *Scheduler) worker(id int) {
	defer s.wg.Done() // Signal WaitGroup when this worker exits

	fmt.Printf("Worker %d started\n", id)

	// Loop indefinitely, waiting for ready signals or stop signal
	for {
		select {
		case _, ok := <-s.readySignal: // Wait for a signal that a task is ready
			if !ok {
				// readySignal channel is closed, means all tasks are processed or an error occurred
				fmt.Printf("Worker %d received stop signal, exiting\n", id)
				return // Exit the worker goroutine
			}

			// Signal received, try to get a task from the priority queue
			s.mu.Lock()
			if s.readyQueue.Len() == 0 {
				// This can happen if multiple workers get a signal but only one task was added
				// Or if the last task was taken just before we acquired the lock.
				// In this case, we just release the lock and wait for the next signal.
				s.mu.Unlock()
				continue
			}

			item := heap.Pop(s.readyQueue).(*PriorityQueueItem)
			task := item.task // task is already *Task here
			s.mu.Unlock()

			// Check if task is already running (shouldn't happen with dependencyCounts)
			if _, loaded := s.runningTasks.LoadOrStore(task.ID, true); loaded {
				fmt.Printf("Worker %d: Task %s already running (should not happen), skipping\n", id, task.ID)
				continue // Defensive check
			}

			fmt.Printf("Worker %d executing task: %s (Priority: %d, Duration: %s)\n", id, task.ID, task.priority, task.Duration)

			// Simulate task execution
			time.Sleep(task.Duration)

			// Simulate potential error (optional)
			// if task.ID == "TaskC" { // Example failure
			// 	fmt.Printf("Worker %d: Task %s failed!\n", id, task.ID)
			// 	s.runningTasks.Delete(task.ID) // Remove from running set
			// 	// In a real scenario, error handling would be more complex here.
			// 	// We would likely signal failure back to the monitor or main loop
			// 	continue // Skip completion logic
			// }

			// Mark task as completed
			s.runningTasks.Delete(task.ID) // Remove from running set
			s.completedTasks <- task       // Signal completion

		}
	}
}

// completionMonitor goroutine
func (s *Scheduler) completionMonitor() {
	// This loop will run until s.completedTasks is closed and drained
	for completedTask := range s.completedTasks {
		s.mu.Lock() // Protect shared dependencyCounts, readyQueue, completedTaskCount
		s.completedTaskCount++
		fmt.Printf("Completed task: %s (Total completed: %d/%d)\n", completedTask.ID, s.completedTaskCount, s.totalTaskCount)

		// Update dependency counts for dependent tasks
		for _, dependentID := range completedTask.Dependents {
			s.dependencyCounts[dependentID]--
			if s.dependencyCounts[dependentID] == 0 {
				// Task is now ready, add it to the priority queue
				// Modified: Retrieve *Task directly
				readyTask, ok := s.dag.Tasks[dependentID]
				if !ok {
					// This should not happen in a valid DAG execution
					fmt.Printf("Error: Dependent task '%s' not found in DAG map!\n", dependentID)
					continue
				}
				heap.Push(s.readyQueue, &PriorityQueueItem{task: readyTask, priority: readyTask.priority})
				fmt.Printf("Task '%s' is now ready (Priority: %d)\n", readyTask.ID, readyTask.priority)

				// Signal a worker that there's a new task in the queue
				select {
				case s.readySignal <- struct{}{}:
					// Signal sent
				default:
					// Channel is full, workers are likely busy or already signaled
					// No need to block here, signals will queue up or workers will poll
				}
			}
		}
		s.mu.Unlock()

		// If all tasks are completed, close channels to signal workers to stop
		if s.completedTaskCount == s.totalTaskCount {
			// Give workers a moment to potentially read the last signals
			// (Though closing readySignal is the primary stop mechanism)
			// time.Sleep(10 * time.Millisecond) // Optional brief pause

			close(s.readySignal)    // Signal workers to exit their loop
			close(s.completedTasks) // Stop this monitor loop (optional, but good practice)
			return                  // Exit monitoring goroutine
		}
	}
	// This part is reached if completedTasks channel is closed before all tasks are done (e.g., due to an error)
	fmt.Println("Completion monitor exiting.")
}

// Helper function to create a task
func NewTask(id string, duration time.Duration, f func() error) *Task {
	if f == nil {
		// Default function if none provided
		f = func() error {
			// fmt.Printf("Executing default function for %s\n", id) // Optional: uncomment for more verbose logging
			return nil
		}
	}
	return &Task{
		ID:          id,
		ExecuteFunc: f,
		Duration:    duration,
	}
}

func main() {
	// Example Usage: Create a DAG with parallelizable tasks
	dag := NewDAG()

	// Define tasks with simulated durations
	// Let's use durations that highlight critical path differences
	taskA := NewTask("TaskA", 5000*time.Millisecond, nil) // Path A -> D -> F (500+600+800 = 1900)
	taskB := NewTask("TaskB", 7000*time.Millisecond, nil) // Path B -> E -> F (700+400+800 = 1900)
	taskC := NewTask("TaskC", 3000*time.Millisecond, nil) // Path C -> D -> F (300+600+800 = 1700), Path C -> E -> F (300+400+800 = 1500)
	// Max from C is 1700.
	taskD := NewTask("TaskD", 600*time.Millisecond, nil) // Path D -> F (600+800 = 1400)
	taskE := NewTask("TaskE", 400*time.Millisecond, nil) // Path E -> F (400+800 = 1200)
	taskF := NewTask("TaskF", 800*time.Millisecond, nil) // Sink node (800)

	// Add tasks to the DAG
	dag.AddTask(taskA)
	dag.AddTask(taskB)
	dag.AddTask(taskC)
	dag.AddTask(taskD)
	dag.AddTask(taskE)
	dag.AddTask(taskF)

	// Define dependencies
	dag.AddDependency("TaskA", "TaskD")
	dag.AddDependency("TaskC", "TaskD")

	dag.AddDependency("TaskB", "TaskE")
	dag.AddDependency("TaskC", "TaskE")

	dag.AddDependency("TaskD", "TaskF")
	dag.AddDependency("TaskE", "TaskF")

	fmt.Println("DAG Structure:")
	for id, task := range dag.Tasks { // Modified: Iterate over *Task directly
		fmt.Printf("Task %s: Dependencies %v, Dependents %v, Duration %s\n",
			id, task.Dependencies, task.Dependents, task.Duration)
	}
	fmt.Println("--- Starting Scheduler (Critical Path Optimized) ---")

	// Create a scheduler with a worker pool size
	workerPoolSize := 1 // Adjust based on your available cores/desired parallelism
	scheduler := NewScheduler(dag, workerPoolSize)

	// Run the scheduler
	startTime := time.Now()
	err := scheduler.Run()
	if err != nil {
		fmt.Printf("Error running scheduler: %v\n", err)
		return
	}
	endTime := time.Now()

	fmt.Println("--- Scheduler Finished ---")
	fmt.Printf("Total execution time: %s\n", endTime.Sub(startTime))
}
