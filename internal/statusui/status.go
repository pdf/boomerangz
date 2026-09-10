// Package statusui renders control snapshots for terminals and structured output.
package statusui

import (
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	controlrpc "github.com/pdf/boomerangz/internal/control/rpc"
)

type output struct {
	Revision         uint64                      `json:"revision"`
	Observed         time.Time                   `json:"observed"`
	Generation       uint64                      `json:"generation"`
	ConfigGeneration uint64                      `json:"config_generation"`
	Datasets         []*controlrpc.DatasetStatus `json:"datasets"`
	Queues           []*controlrpc.QueueStatus   `json:"queues"`
	Jobs             []*controlrpc.JobStatus     `json:"jobs"`
}

func normalized(snapshot *controlrpc.StatusSnapshot) output {
	result := output{Revision: snapshot.GetRevision(), Generation: snapshot.GetGeneration(), ConfigGeneration: snapshot.GetConfigGeneration(), Datasets: slices.Clone(snapshot.GetDatasets()), Queues: slices.Clone(snapshot.GetQueues()), Jobs: slices.Clone(snapshot.GetJobs())}
	if snapshot.GetObservedUnixNano() != 0 {
		result.Observed = time.Unix(0, snapshot.GetObservedUnixNano()).UTC()
	}
	slices.SortFunc(result.Datasets, func(a, b *controlrpc.DatasetStatus) int { return strings.Compare(a.GetName(), b.GetName()) })
	slices.SortFunc(result.Queues, func(a, b *controlrpc.QueueStatus) int { return strings.Compare(a.GetName(), b.GetName()) })
	slices.SortFunc(result.Jobs, func(a, b *controlrpc.JobStatus) int { return strings.Compare(a.GetJob(), b.GetJob()) })
	return result
}

// JSON writes one stable JSON object followed by a newline.
func JSON(writer io.Writer, snapshot *controlrpc.StatusSnapshot) error {
	return json.NewEncoder(writer).Encode(normalized(snapshot))
}

func bar(pending, capacity uint32, width int) string {
	if width < 4 {
		return ""
	}
	filled := 0
	if capacity != 0 {
		filled = int(uint64(pending) * uint64(width) / uint64(capacity))
	}
	if filled > width {
		filled = width
	}
	return "[" + strings.Repeat("=", filled) + strings.Repeat(" ", width-filled) + "]"
}

func progressBar(completed, total uint64, width int) string {
	if width < 4 || total == 0 {
		return ""
	}
	filled := int(float64(min(completed, total)) / float64(total) * float64(width))
	return "[" + strings.Repeat("=", filled) + strings.Repeat(" ", width-filled) + "]"
}

func byteSize(bytes uint64) string {
	const unit = uint64(1024)
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	value := float64(bytes)
	for _, suffix := range []string{"KiB", "MiB", "GiB", "TiB"} {
		value /= float64(unit)
		if value < float64(unit) {
			return fmt.Sprintf("%.1f %s", value, suffix)
		}
	}
	return fmt.Sprintf("%.1f PiB", value/float64(unit))
}

func newTable(writer io.Writer) *tabwriter.Writer {
	return tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
}

func displayJobName(name string) string {
	remainder, ok := strings.CutPrefix(name, "inactive:")
	if !ok {
		return name
	}
	if dataset, found := strings.CutSuffix(remainder, ":true"); found {
		return "activate:" + dataset
	}
	if dataset, found := strings.CutSuffix(remainder, ":false"); found {
		return "deactivate:" + dataset
	}
	return name
}

// Terminal writes a compact status display that degrades at narrow widths.
func Terminal(writer io.Writer, snapshot *controlrpc.StatusSnapshot, width int) error {
	view := normalized(snapshot)
	if width <= 0 {
		width = 80
	}
	if _, err := fmt.Fprintf(writer, "boomerangz  dataset generation %d  config generation %d  %s\n", view.Generation, view.ConfigGeneration, view.Observed.Format(time.RFC3339)); err != nil {
		return err
	}
	if len(view.Datasets) == 0 {
		if _, err := fmt.Fprintln(writer, "No managed datasets."); err != nil {
			return err
		}
	} else {
		if _, err := fmt.Fprintln(writer); err != nil {
			return err
		}
		table := newTable(writer)
		if _, err := fmt.Fprintln(table, "DATASET\tSTATE\tNEXT SNAPSHOT"); err != nil {
			return err
		}
		for _, dataset := range view.Datasets {
			state, next := "inactive", "-"
			if dataset.GetActive() {
				state = "active"
			}
			if dataset.GetNextSnapshotUnixNano() != 0 {
				next = time.Unix(0, dataset.GetNextSnapshotUnixNano()).UTC().Format(time.RFC3339)
			}
			if _, err := fmt.Fprintf(table, "%s\t%s\t%s\n", dataset.GetName(), state, next); err != nil {
				return err
			}
		}
		if err := table.Flush(); err != nil {
			return err
		}
	}
	if len(view.Queues) != 0 {
		if _, err := fmt.Fprintln(writer, "\nQUEUES"); err != nil {
			return err
		}
		barWidth := width - 34
		table := newTable(writer)
		for _, queue := range view.Queues {
			if barWidth >= 8 {
				if _, err := fmt.Fprintf(table, "%s\t%d/%d\t%s\n", queue.GetName(), queue.GetPending(), queue.GetCapacity(), bar(queue.GetPending(), queue.GetCapacity(), barWidth)); err != nil {
					return err
				}
			} else if _, err := fmt.Fprintf(table, "%s\t%d/%d\n", queue.GetName(), queue.GetPending(), queue.GetCapacity()); err != nil {
				return err
			}
		}
		if err := table.Flush(); err != nil {
			return err
		}
	}
	jobs := slices.DeleteFunc(slices.Clone(view.Jobs), func(job *controlrpc.JobStatus) bool {
		return job.GetState() == "cancelled"
	})
	if len(jobs) != 0 {
		if _, err := fmt.Fprintln(writer, "\nLATEST WORK"); err != nil {
			return err
		}
		jobWidth, stateWidth := 0, 0
		for _, job := range jobs {
			jobWidth = max(jobWidth, len(displayJobName(job.GetJob())))
			stateWidth = max(stateWidth, len(job.GetState()))
		}
		var rows strings.Builder
		table := newTable(&rows)
		for _, job := range jobs {
			var detail strings.Builder
			if job.GetState() == "sending" && job.GetTotalKnown() && job.GetTotalBytes() != 0 {
				progressWidth := width - jobWidth - stateWidth - 48
				if progressWidth >= 8 {
					detail.WriteString(progressBar(job.GetBytes(), job.GetTotalBytes(), progressWidth))
					detail.WriteByte(' ')
				}
				_, _ = fmt.Fprintf(&detail, "%s/%s", byteSize(job.GetBytes()), byteSize(job.GetTotalBytes()))
			} else if job.GetState() == "sending" && job.GetBytes() != 0 {
				detail.WriteString(byteSize(job.GetBytes()))
			}
			if job.GetState() == "sending" && job.GetBytesPerSecond() > 0 {
				_, _ = fmt.Fprintf(&detail, " @ %s/s", byteSize(uint64(job.GetBytesPerSecond())))
			}
			if job.GetState() == "sending" && job.GetEtaNanoseconds() > 0 {
				detail.WriteString(" ETA " + time.Duration(job.GetEtaNanoseconds()).Round(time.Second).String())
			}
			if job.GetReason() != "" {
				if detail.Len() != 0 {
					detail.WriteByte(' ')
				}
				detail.WriteString(job.GetReason())
			}
			if _, err := fmt.Fprintf(table, "%s\t%s\t%s\n", displayJobName(job.GetJob()), job.GetState(), detail.String()); err != nil {
				return err
			}
		}
		if err := table.Flush(); err != nil {
			return err
		}
		for line := range strings.Lines(rows.String()) {
			line = strings.TrimSuffix(line, "\n")
			if len(line) > width {
				line = line[:width]
			}
			if _, err := fmt.Fprintln(writer, line); err != nil {
				return err
			}
		}
	}
	return nil
}
