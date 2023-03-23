package allocate

import (
	"volcano.sh/volcano/pkg/scheduler/api"
)

func getOneTask(job *api.JobInfo) *api.TaskInfo {
	for _, t := range job.TaskStatusIndex[api.Pending] {
		return t
	}
	return nil
}
