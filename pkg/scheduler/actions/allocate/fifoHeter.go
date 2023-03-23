package allocate

import (
	"volcano.sh/volcano/pkg/scheduler/api"
)

func getFreeNode(nodes []*api.NodeInfo, task *api.TaskInfo, jobType string) map[string][]*api.NodeInfo {
	freeNodes := make(map[string][]*api.NodeInfo)

	if jobType == "GPU" {
		freeNodes = getGPUFreeNode(nodes, task)
	}
	if jobType == "MPI" {
		freeNodes = getRackFreeNode(nodes, task)
	}

	return freeNodes
}

func getGPUFreeNode(nodes []*api.NodeInfo, task *api.TaskInfo) map[string][]*api.NodeInfo {
	freeNodes := make(map[string][]*api.NodeInfo)
	for _, node := range nodes {
		if len(node.Tasks) == 0 {

			if gpu, found := node.Node.ObjectMeta.Labels["GPU"]; found && gpu == "true" {
				freeNodes["fast"] = append(freeNodes["fast"], node)
			} else {
				freeNodes["slow"] = append(freeNodes["slow"], node)
			}

		}
	}
	return freeNodes
}
func getRackFreeNode(nodes []*api.NodeInfo, task *api.TaskInfo) map[string][]*api.NodeInfo {
	freeNodes := make(map[string][]*api.NodeInfo)
	for _, node := range nodes {
		if len(node.Tasks) == 0 {
			if rack, found := node.Node.ObjectMeta.Labels["Rack"]; found {
				freeNodes[rack] = append(freeNodes[rack], node)
			}
		}
	}
	return freeNodes
}

func allocateJob(job *api.JobInfo, freeNode map[string][]*api.NodeInfo, jobAlloc map[*api.TaskInfo]*api.NodeInfo, jobType string) bool {

	if jobType == "MPI" {
		return allocateMPI(job, freeNode, jobAlloc)
	} else if jobType == "GPU" {
		return allocateGPU(job, freeNode, jobAlloc)
	}
	return false
}

func allocateFallback(job *api.JobInfo, nodes []*api.NodeInfo, jobAlloc map[*api.TaskInfo]*api.NodeInfo) bool {
	tasks := job.TaskStatusIndex[api.Pending]
	tmpused := make(map[*api.NodeInfo]bool)
	tmpJobAlloc := make(map[*api.TaskInfo]*api.NodeInfo)
	for _, t := range tasks {
		for _, n := range nodes {
			if len(n.Tasks) == 0 && !tmpused[n] {
				tmpJobAlloc[t] = n
				tmpused[n] = true

				break
			}
		}
	}

	if len(tasks) != len(tmpJobAlloc) {
		return false
	}
	for t, n := range tmpJobAlloc {
		jobAlloc[t] = n
	}
	return true
}

func allocateGPU(job *api.JobInfo, freeNode map[string][]*api.NodeInfo, jobAlloc map[*api.TaskInfo]*api.NodeInfo) bool {
	tasks := job.TaskStatusIndex[api.Pending]
	i := 0

	if len(tasks) > len(freeNode["fast"]) {
		return false
	}
	for _, task := range tasks {
		jobAlloc[task] = freeNode["fast"][i]
		i++
	}

	return true
}

func allocateMPI(job *api.JobInfo, freeNode map[string][]*api.NodeInfo, jobAlloc map[*api.TaskInfo]*api.NodeInfo) bool {
	tasks := job.TaskStatusIndex[api.Pending]
	for _, rnodes := range freeNode {
		if len(rnodes) >= len(tasks) {
			i := 0
			for _, task := range tasks {
				jobAlloc[task] = rnodes[i]
				i++
			}
			break
		}
	}
	if len(jobAlloc) != len(tasks) {

		return false
	}
	return true
}

func fifoHeterFn(jobs []*api.JobInfo, nodes []*api.NodeInfo) map[*api.TaskInfo]*api.NodeInfo {
	allocation := make(map[*api.TaskInfo]*api.NodeInfo)
	if len(jobs) == 0 {
		return allocation
	}

	job := jobs[0]
	task := getOneTask(job)
	jobType := task.Pod.ObjectMeta.Labels["type"]
	freeNode := getFreeNode(nodes, task, jobType)
	jobAlloc := make(map[*api.TaskInfo]*api.NodeInfo)
	if allocateJob(job, freeNode, jobAlloc, jobType) || allocateFallback(job, nodes, jobAlloc) {

		for t, n := range jobAlloc {
			allocation[t] = n
		}
	}
	return allocation

}
