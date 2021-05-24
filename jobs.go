package main

import (
	"log"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

type JobsMetrics struct {
	jobPartition        string
	jobName             string
	jobUser             string
	jobState            string
	jobRunningTime      uint64
	jobNodesCount       uint64
	jobNodeList         string
	jobNodeListExpanded string
}

func JobsGetMetrics() map[string]*JobsMetrics {
	return ParseJobsMetrics(JobData())
}

// ParseNodeMetrics takes the output of squeue with job data
// It returns a map of metrics per job
func ParseJobsMetrics(input []byte) map[string]*JobsMetrics {
	jobs := make(map[string]*JobsMetrics)
	lines := strings.Split(string(input), "\n")

	// Sort and remove all the dublicates from the 'squeue' output
	sort.Strings(lines)
	linesUniq := RemoveDuplicates(lines)

	for _, line := range linesUniq {
		job := strings.Split(line, "|")
		jobId := job[0]

		jobs[jobId] = &JobsMetrics{"", "", "", "", 0, 0, "", ""}
		jobs[jobId].jobPartition = job[1]
		jobs[jobId].jobName = job[2]
		jobs[jobId].jobUser = job[3]
		jobs[jobId].jobState = job[4]
		jobs[jobId].jobRunningTime = RunningTimeToSeconds(job[6])
		jobs[jobId].jobNodesCount, _ = strconv.ParseUint(job[7], 10, 64)
		jobs[jobId].jobNodeList = job[8]
		jobs[jobId].jobNodeListExpanded = strings.Join(expandNodeList(job[8]), ", ")
	}

	return jobs
}

func expandNodeList(collapsed string) []string {
	var output []string
	var splitRangesRegex = regexp.MustCompile(`[-\w.]+\[.*?\]|[-\w.]+`)
	var splitRanges = splitRangesRegex.FindAllString(collapsed, -1)

	for _, n := range splitRanges {
		if regexp.MustCompile(`\[`).MatchString(n) {
			splitted := regexp.MustCompile(`(?P<prefix>.*)\[(?P<postfix>.*)\]`).FindStringSubmatch(n)
			pre := splitted[1]
			post := splitted[2]
			x := strings.Split(post, ",")
			var temp []string

			for _, y := range x {
				if !strings.Contains(y, "-") {
					temp = append(temp, pre+y)
				} else {
					last := strings.Split(y, "-")[1]
					first := strings.Split(y, "-")[0]
					lastIterator, _ := strconv.Atoi(last)
					for firstIterator, _ := strconv.Atoi(first); firstIterator < lastIterator+1; firstIterator++ {
						var lZ string
						firstIteratorStr := strconv.Itoa(firstIterator)
						for z := len(firstIteratorStr); z < len(last); z++ {
							lZ = lZ + "0"
						}
						temp = append(temp, pre+lZ+firstIteratorStr)
					}
				}
			}
			output = append(output, temp...)
		} else {
			output = append(output, n)
		}
	}

	return output
}

func RunningTimeToSeconds(input string) uint64 {
	var seconds uint64
	const sixty uint64 = 60
	const twentyfour uint64 = 24
	raw := regexp.MustCompile("[\\-\\,\\:\\s]+").Split(input, -1)

	var rawInts = []uint64{}
	for _, i := range raw {
		j, err := strconv.ParseUint(i, 10, 64)
		if err != nil {
			panic(err)
		}
		rawInts = append(rawInts, j)
	}

	switch timeLength := len(rawInts); {
	case timeLength == 2:
		seconds = rawInts[0]*sixty + rawInts[1]
	case timeLength == 3:
		seconds = rawInts[0]*sixty*sixty + rawInts[1]*sixty + rawInts[2]
	case timeLength == 4:
		seconds = rawInts[0]*twentyfour*sixty*sixty + rawInts[1]*sixty*sixty + rawInts[2]*sixty + rawInts[3]
	}

	return seconds
}

func JobData() []byte {
	//make this cleaner?
	cmd := exec.Command("squeue", "-h", "--states=R", "-o", "%i|%P|%j|%u|%T|%S|%M|%D|%N")
	out, err := cmd.Output()
	if err != nil {
		log.Fatal(err)
	}

	return out
}

type JobsCollector struct {
	jobRunningTime *prometheus.Desc
	jobNodesCount  *prometheus.Desc
}

func NewJobsCollector() *JobsCollector {
	labels := []string{"job", "partition", "name", "user", "nodeslist", "nodelistexpanded"}

	return &JobsCollector{
		jobRunningTime: prometheus.NewDesc("job_running_sec", "Time a running job running has spent run until now, in seconds", labels, nil),
		jobNodesCount:  prometheus.NewDesc("job_running_nodescount", "Number of nodes a running job has allocated", labels, nil),
	}
}

func (jc *JobsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- jc.jobRunningTime
	ch <- jc.jobNodesCount
}

func (jc *JobsCollector) Collect(ch chan<- prometheus.Metric) {
	jobs := JobsGetMetrics()
	for job := range jobs {
		ch <- prometheus.MustNewConstMetric(jc.jobRunningTime, prometheus.CounterValue, float64(jobs[job].jobRunningTime), job, jobs[job].jobPartition, jobs[job].jobName, jobs[job].jobUser, jobs[job].jobNodeList, jobs[job].jobNodeListExpanded)
		ch <- prometheus.MustNewConstMetric(jc.jobNodesCount, prometheus.CounterValue, float64(jobs[job].jobNodesCount), job, jobs[job].jobPartition, jobs[job].jobName, jobs[job].jobUser, jobs[job].jobNodeList, jobs[job].jobNodeListExpanded)
	}
}
