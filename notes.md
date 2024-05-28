adaptivity with feedback - the overall token generation rate can be adjusted dynamically based on external factors
fairness - tasks can be assigned groups and the amount of resources supplied to each group can be monitored. If they becom unbalanced the priorities are adjusted to balance them.
quotas - different task groups can be granted higher or smaller allocations of resources in the form of how token generation rate
timeout - tasks have a maximum execution time after which they will be canceled and marked as failed
task dependency
    - maturation times are propagated down and deadlines are propagated up to determine whether task chains are possible
    - job time estimates allow adaptive prioritization based on deadlines for down stream tasks
load shedding - the list of queued tasks has a maximum size. If that size is reached, low priority tasks are marked as failed and removed from the queue
    - if the queue is full, the maximum rate at which jobs can be added can be decreased


## Parameters
### Task Parameters
1. Assigned Priority: The initial priority value of the task (non-negative floating point number).
2. Effective Priority: Calculated priority considering aging, deadlines, and other factors.
3. Aging Rate: The rate at which the task's priority increases over time.
4. Maturation Time: The time at which the task can start executing.
5. Deadline: The latest time by which the task must start; otherwise, it is marked as failed.
6. Token Cost: The number of tokens required for the task to execute, based on resource needs.
7. Maximum Execution Time: The maximum allowed execution time for the task before it is canceled (timeout).
8. Dependencies: Other tasks that must be completed before this task can start.
9. Job Time Estimate: Estimated execution time for the task, used for adaptive prioritization.

### Task Group Parameters

1. Group Priority Adjustment: Adjustments to priorities for balancing resource allocation among groups.
2. Token Bucket Size: The maximum number of tokens available for the group.
3. Token Refill Rate: The rate at which tokens are added to the group's bucket.
4. Hard Rate Limit: The maximum rate at which tasks from the group can be executed to handle bursts.
5. Quota: The allocated share of resources (tokens) for the group.
6. Fairness Adjustment: Mechanism to adjust priorities to ensure fair resource distribution among groups.

### Scheduler Parameters

1. Global Token Refill Rate: The overall rate at which tokens are generated for all groups.
2. Global Hard Rate Limit: The maximum rate of task execution across all groups.
3. Maximum Queue Size: The maximum number of tasks that can be queued before load shedding occurs.
4. Load Shedding Threshold: The priority threshold below which tasks will be marked as failed and removed from the queue when the maximum queue size is reached.
5. Adaptivity Feedback Mechanisms: Parameters to adjust token generation rates and other factors based on system performance and external factors.
6. Priority Calculation Factors: Weights for aging, deadlines, and other factors in calculating effective priority.
7. Topological Sort Maintenance: Parameters for maintaining and updating the topological sort of tasks to prevent cyclic dependencies.
8. Propagation Rules: Rules for propagating maturation times and deadlines through the dependency graph.

## Variables
### Task Variables

1. Assigned Priority: The initial priority value assigned to the task.
2. Effective Priority: The current priority of the task after considering aging, deadlines, and other adjustments.
3. Age: The amount of time the task has been in the queue, used for aging calculations.
4. Maturation Time: The time at which the task is allowed to start.
5. Deadline: The latest time by which the task must start.
6. Token Cost: The number of tokens required for the task to execute.
7. Start Time: The time when the task starts execution.
8. Execution Time: The duration for which the task has been running.
9. Dependencies: A list of tasks that must be completed before this task can start.
10. State: The current state of the task (queued, running, completed, failed).
11. Job Time Estimate: The estimated time required to complete the task.

### Task Group Variables

1. Token Bucket Size: The current number of tokens available in the group's bucket.
2. Token Refill Rate: The rate at which tokens are added to the group's bucket.
3. Hard Rate Limit: The maximum rate at which tasks from the group can be executed.
4. Quota Utilization: The amount of allocated resources (tokens) used by the group.
5. Fairness Metrics: Metrics to ensure fair resource distribution among groups, such as average wait time, resource usage, and number of completed tasks.
6. Group Priority Adjustment: Adjustments made to task priorities within the group for fairness.
7. Number of Tasks: The current number of tasks in the group, categorized by their state (queued, running, completed, failed).

### Scheduler Variables

1. Global Token Refill Rate: The overall rate at which tokens are generated for all groups.
2. Global Hard Rate Limit: The maximum rate of task execution across all groups.
3. Total Queue Size: The total number of tasks in the queue.
4. Load Shedding Threshold: The priority threshold below which tasks will be marked as failed and removed from the queue when the maximum queue size is reached.
5. System Load Metrics: Metrics such as CPU usage, memory usage, and I/O usage to monitor system load.
6. Adaptivity Feedback: Data used to adjust token generation rates and other factors based on system performance and external conditions.
7. Task Throughput: The rate at which tasks are completed.
8. Task Latency: The average time tasks spend in the system from arrival to completion.
9. Failed Tasks: The number and proportion of tasks that have failed due to missed deadlines, timeouts, or load shedding.
10. Topological Sort State: The current state of the task dependency graph and its topological order.
11. Propagation Data: Information about the propagation of maturation times and deadlines through the dependency graph.
12. Priority Calculation Data: Weights and factors used in the effective priority calculation.

## Data Structures
1. Priority Queue - manage manage priority-order sorting
2. Min-Heap - Maturation tracking
3. Min-Heap - Deadlines tracking
4. Directed Acyclic Graph - dependency tracking
4. Topological Sort State
5. Hash Table - Task Lookup
1. Token Bucket per Group - dict[Group, int]
2. Quota Tracking - dict[Group, float] - token generation rates
3. Fairness Metrics - dict[Group, Any]
4. Global token refil rate - float
3. System Load Metrics

## Policies
- Priority Adjustment Frequency: during push and just before pull

- Effective Priority Calculation: Define the formula for calculating the effective priority, incorporating aging, deadlines, and any other factors.
- Minimum Effective Priority: Specify the minimum effective priority to ensure low-priority tasks do not unfairly override high-priority tasks.
- Maturation Time Propagation: Define how maturation times are propagated through the task dependency graph.
- Deadline Propagation: Define how deadlines are propagated up the task dependency graph.
- Token Consumption: Specify the rules for how tasks consume tokens based on their resource needs.

- Token Generation Adjustments: Define the feedback mechanism for adjusting the token generation rate dynamically.
- Fairness Adjustment Mechanism: Specify how and when the system adjusts priorities to ensure fairness among task groups.
- Quota Enforcement: Define the rules for enforcing quotas for different task groups and how violations are handled.
- Timeout Handling Actions: Specify what happens when a task exceeds its maximum execution time (e.g., cancellation and marking as failed).

- Cyclic Dependency Detection: Define the algorithm for detecting and rejecting tasks that would cause cyclic dependencies.
- Topological Sort Maintenance: Specify how the topological order of tasks is maintained and updated dynamically.
- Load Shedding Actions: Define the actions taken when load shedding occurs (e.g., marking tasks as failed and adjusting job addition rates).

- Load Metrics Collection: Specify how system load metrics (e.g., CPU usage, memory usage, I/O usage) are collected and monitored.
- Load Impact on Scheduling: Define how system load metrics influence scheduling decisions and adjustments.

- Token Distribution and redistribution: Specify the initial state of token buckets and quotas.
- Task Completion Handling: Define how completed tasks are removed from the system and how resources are reallocated.
- Graceful Shutdown Procedure: Specify the steps for gracefully shutting down the algorithm, ensuring that all in-progress tasks are handled appropriately.
