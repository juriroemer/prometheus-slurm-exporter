package main

import (
	"log"
	"os/exec"
	"sort"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

type JobsMetrics struct {
	jobPartition string
	jobName      string
	jobUser      string
	jobState     string 
	jobRunningTime uint64
	jobNodesCount  uint64
	jobNodeList    string
	// TODO: write nodelist parser!
	// jobVarNodes []string
}

func JobsGetMetrics() map[uint64]*JobsMetrics {
	return ParseJobsMetrics(JobData())
}

// ParseNodeMetrics takes the output of squeue with job data
// It returns a map of metrics per job
func ParseJobsMetrics(input []byte) map[uint64]*JobsMetrics {
	jobs := make(map[uint64]*JobsMetrics)
	lines := strings.Split(string(input), "\n")

	// Sort and remove all the dublicates from the 'squeue' output
	sort.Strings(lines)
	linesUniq := RemoveDuplicates(lines)

	for _, line := range linesUniq[1:] {
		job := strings.Fields(line)
		jobId, _ := strconv.ParseUint(job[0], 10, 64)

		jobs[jobId] = &JobsMetrics{"", "", "", "", 0, 0, ""}

		jobPartition := job[1]
		jobName := job[2]
		jobUser := job[3]
		jobState := job[4]
		// what about start time?
		jobRunningTime := RunningTimeToSeconds(job[6])
		jobNodesCount, _ := strconv.ParseUint(job[7], 10, 64)
		jobNodeList := job[8]

		jobs[jobId].jobPartition = jobPartition
		jobs[jobId].jobName = jobName
		jobs[jobId].jobUser = jobUser
		jobs[jobId].jobState = jobState
		jobs[jobId].jobRunningTime = jobRunningTime
		jobs[jobId].jobNodesCount = jobNodesCount
		jobs[jobId].jobNodeList = jobNodeList
	}

	return jobs
}

func JobData() []byte {
	//make this cleaner?
	cmd := exec.Command("squeue", "--state=R", "-o", "%i", "%P", "%j", "%u", "%T", "%S", "%M", "%D", "%N")
	out, err := cmd.Output()
	if err != nil {
		log.Fatal(err)
	}
	return out
}

type JobsCollector struct {
	jobRunningTime *prometheus.Desc
}

func NewJobsCollector() *JobsCollector {
	labels := []string{"partition", "name", "user", "state", "nodescount", "nodeslist"}

	return &JobsCollector{
		jobRunningTime: prometheus.NewDesc("job_running_time", "Time a job running has spent run until now", labels, nil)
	}
}

func (jc *JobsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- jc.jobRunningTime
}

func (jc *JobsCollector) Collect(ch chan<- prometheus.Metric){
	jobs := JobsGetMetrics()
	for job := range jobs {
		ch <- prometheus.MustNewConstMetric(jc.jobRunningTime, prometheus.CounterValue, float64(jobs[job].jobRunningTime), job, jobs[job].jobPartition, jobs[job].jobName, jobs[job].jobUser, jobs[job].jobState, jobs[job].jobNodesCount, jobs[job].jobNodeList)
	}
}