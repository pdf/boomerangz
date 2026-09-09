// Package statusui renders control snapshots for terminals and structured output.
package statusui

import (
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
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
		if _, err := fmt.Fprintln(writer, "\nDATASET\tSTATE\tNEXT SNAPSHOT"); err != nil {
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
			if _, err := fmt.Fprintf(writer, "%s\t%s\t%s\n", dataset.GetName(), state, next); err != nil {
				return err
			}
		}
	}
	if len(view.Queues) != 0 {
		if _, err := fmt.Fprintln(writer, "\nQUEUES"); err != nil {
			return err
		}
		barWidth := width - 34
		for _, queue := range view.Queues {
			if barWidth >= 8 {
				if _, err := fmt.Fprintf(writer, "%-18s %4d/%-4d %s\n", queue.GetName(), queue.GetPending(), queue.GetCapacity(), bar(queue.GetPending(), queue.GetCapacity(), barWidth)); err != nil {
					return err
				}
			} else if _, err := fmt.Fprintf(writer, "%s %d/%d\n", queue.GetName(), queue.GetPending(), queue.GetCapacity()); err != nil {
				return err
			}
		}
	}
	if len(view.Jobs) != 0 {
		if _, err := fmt.Fprintln(writer, "\nLATEST WORK"); err != nil {
			return err
		}
		for _, job := range view.Jobs {
			line := fmt.Sprintf("%-28s %-16s", job.GetJob(), job.GetState())
			if job.GetState() == "sending" && job.GetTotalKnown() && job.GetTotalBytes() != 0 {
				progressWidth := width - len(line) - 44
				if progressWidth >= 8 {
					line += " " + progressBar(job.GetBytes(), job.GetTotalBytes(), progressWidth)
				}
				line += fmt.Sprintf(" %s/%s", byteSize(job.GetBytes()), byteSize(job.GetTotalBytes()))
			} else if job.GetState() == "sending" && job.GetBytes() != 0 {
				line += " " + byteSize(job.GetBytes())
			}
			if job.GetState() == "sending" && job.GetBytesPerSecond() > 0 {
				line += fmt.Sprintf(" @ %s/s", byteSize(uint64(job.GetBytesPerSecond())))
			}
			if job.GetState() == "sending" && job.GetEtaNanoseconds() > 0 {
				line += " ETA " + time.Duration(job.GetEtaNanoseconds()).Round(time.Second).String()
			}
			if job.GetReason() != "" {
				line += " " + job.GetReason()
			}
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
