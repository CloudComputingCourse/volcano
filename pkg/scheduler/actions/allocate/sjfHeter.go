package allocate

import (
	"math"
	"strconv"

	"volcano.sh/volcano/pkg/scheduler/api"
)

func sjfHeterFn(jobs []*api.JobInfo, nodes []*api.NodeInfo) map[*api.TaskInfo]*api.NodeInfo {

	allocation := make(map[*api.TaskInfo]*api.NodeInfo)

	shortestTime := math.MaxInt32

	var resMachines map[*api.TaskInfo]*api.NodeInfo
	for _, job := range jobs {

		task := getOneTask(job)
		jobType := task.Pod.ObjectMeta.Labels["type"]
		fastDuration, _ := strconv.Atoi(task.Pod.ObjectMeta.Labels["FastDuration"])
		slowDuration, _ := strconv.Atoi(task.Pod.ObjectMeta.Labels["SlowDuration"])
		freeNode := getFreeNode(nodes, task, jobType)
		jobAlloc := make(map[*api.TaskInfo]*api.NodeInfo)
		if allocateJob(job, freeNode, jobAlloc, jobType) && fastDuration < shortestTime {
			shortestTime = fastDuration
			resMachines = make(map[*api.TaskInfo]*api.NodeInfo)
			for t, n := range jobAlloc {
				resMachines[t] = n
			}

		} else if allocateFallback(job, nodes, jobAlloc) && slowDuration < shortestTime {
			shortestTime = slowDuration
			resMachines = make(map[*api.TaskInfo]*api.NodeInfo)
			for t, n := range jobAlloc {
				resMachines[t] = n
			}
		}
	}

	for t, n := range resMachines {
		allocation[t] = n
	}

	return allocation
}
