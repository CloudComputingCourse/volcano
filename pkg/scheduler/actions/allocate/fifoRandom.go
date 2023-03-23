package allocate

import (
	"volcano.sh/volcano/pkg/scheduler/api"
)

func fifoRandomFn(jobs []*api.JobInfo, nodes []*api.NodeInfo) map[*api.TaskInfo]*api.NodeInfo {
	allocation := make(map[*api.TaskInfo]*api.NodeInfo)
	jobAlloc := make(map[*api.TaskInfo]*api.NodeInfo)
	if len(jobs) == 0 {
		return allocation
	}

	job := jobs[0]

	if allocateFallback(job, nodes, jobAlloc) {

		for t, n := range jobAlloc {
			allocation[t] = n
		}
	}

	return allocation
}
