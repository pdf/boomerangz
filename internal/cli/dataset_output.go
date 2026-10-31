package cli

import (
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"text/tabwriter"

	"github.com/pdf/boomerangz/internal/discovery"
	"github.com/pdf/boomerangz/internal/policy"
)

// filterDatasets keeps the entries whose status is among statuses, "all"
// keeping every one. With no statuses it keeps all but inactive datasets and
// reports how many it hid.
func filterDatasets(entries []discovery.Entry, statuses []string) ([]discovery.Entry, int) {
	if slices.Contains(statuses, "all") {
		return entries, 0
	}
	keep := func(status string) bool { return slices.Contains(statuses, status) }
	if len(statuses) == 0 {
		keep = func(status string) bool { return status != "inactive" }
	}
	total := len(entries)
	filtered := slices.DeleteFunc(entries, func(entry discovery.Entry) bool { return !keep(datasetStatus(entry)) })
	if len(statuses) != 0 {
		return filtered, 0
	}
	return filtered, total - len(filtered)
}

// writeDatasetList renders entries as a table. hidden counts the inactive
// datasets the default filter left out, so the table can say how to see them.
func writeDatasetList(writer io.Writer, entries []discovery.Entry, hidden int) error {
	if len(entries) == 0 {
		message := "No datasets found.\n"
		if hidden > 0 {
			message = fmt.Sprintf("No datasets found other than %d inactive; show them with: boomerangz dataset list --status all\n", hidden)
		}
		_, err := io.WriteString(writer, message)
		return err
	}
	var output strings.Builder
	table := tabwriter.NewWriter(&output, 0, 4, 2, ' ', 0)
	blocked := slices.ContainsFunc(entries, func(entry discovery.Entry) bool { return len(entry.Blocked) > 0 })
	if blocked {
		_, _ = fmt.Fprintln(table, "STATUS\tDATASET\tBLOCKED TARGETS")
	} else {
		_, _ = fmt.Fprintln(table, "STATUS\tDATASET")
	}
	invalid := false
	for _, entry := range entries {
		status := datasetStatus(entry)
		invalid = invalid || status == "invalid"
		if len(entry.Blocked) > 0 {
			_, _ = fmt.Fprintf(table, "%s\t%s\t%s\n", status, entry.Dataset.Name, strings.Join(blockedNames(entry), ", "))
		} else {
			_, _ = fmt.Fprintf(table, "%s\t%s\n", status, entry.Dataset.Name)
		}
	}
	if err := table.Flush(); err != nil {
		return err
	}
	switch {
	case invalid && blocked:
		output.WriteString("\nInspect invalid datasets and blocked targets with: boomerangz dataset inspect <dataset>\n")
	case invalid:
		output.WriteString("\nInspect invalid datasets with: boomerangz dataset inspect <dataset>\n")
	case blocked:
		output.WriteString("\nInspect blocked targets with: boomerangz dataset inspect <dataset>\n")
	}
	switch {
	case hidden == 1:
		output.WriteString("\n1 inactive dataset hidden; show it with: boomerangz dataset list --status all\n")
	case hidden > 1:
		_, _ = fmt.Fprintf(&output, "\n%d inactive datasets hidden; show them with: boomerangz dataset list --status all\n", hidden)
	}
	_, err := io.WriteString(writer, output.String())
	return err
}

// blockedNames names an actor root's blocked targets: a local destination root
// as configured, a remote as "remote <name>".
func blockedNames(entry discovery.Entry) []string {
	names := make([]string, 0, len(entry.Blocked))
	for _, blocked := range entry.Blocked {
		if blocked.Remote {
			names = append(names, "remote "+blocked.Target)
		} else {
			names = append(names, blocked.Target)
		}
	}
	return names
}

func datasetStatus(entry discovery.Entry) string {
	if len(entry.Policy.Errors) > 0 {
		return "invalid"
	}
	if entry.CoveredBy != "" && entry.Member {
		return "member"
	}
	if entry.CoveredBy != "" {
		return "excluded"
	}
	if entry.Inspected && entry.Policy.Enabled {
		return "active"
	}
	if entry.Replica != (discovery.Replica{}) {
		return "target"
	}
	return "inactive"
}

func writeDatasetInspection(writer io.Writer, entry discovery.Entry) error {
	var output strings.Builder
	line := func(label, value string) { _, _ = fmt.Fprintf(&output, "%s: %s\n", label, value) }

	line("Dataset", entry.Dataset.Name)
	line("Type", string(entry.Dataset.Type))
	line("Activation", activationStatus(entry))
	line("Policy", validStatus(entry.Policy))
	line("Actor root", actorRoot(entry))
	if entry.Replica != (discovery.Replica{}) {
		line("Replica", replicaOf(entry.Replica))
	}
	if len(entry.Members) > 0 {
		line("Members", strings.Join(entry.Members, ", "))
	}
	line("Encryption root", encryptionRoot(entry.Dataset.EncryptionRoot))
	line("Snapshot policy", entry.Policy.Grid.String())
	line("Local destinations", listOrNone(entry.Policy.Local))
	line("Remote destinations", listOrNone(entry.Policy.Remote))
	line("Incremental mode", entry.Policy.Incremental)
	line("Receive path mapping", string(entry.Policy.Discard))
	line("Requested send features", formatFlags(entry.Policy.Requested))
	line("Effective send features", formatFlags(entry.Policy.Send))

	writeStringMap(&output, "Receive property overrides", entry.Policy.SetProperties)
	writeList(&output, "Receive property exclusions", entry.Policy.IgnoreProperties)

	output.WriteString("\nEffective properties:\n")
	for _, name := range slices.Sorted(maps.Keys(entry.Policy.Values)) {
		value := entry.Policy.Values[name]
		source := "default"
		if value.Dataset != "" {
			source = "set on " + value.Dataset
		}
		_, _ = fmt.Fprintf(&output, "  %s = %s (%s)\n", strings.TrimPrefix(name, policy.Namespace), value.Value, source)
	}

	if len(entry.Stored) == 0 {
		output.WriteString("\nStored properties: none\n")
	} else {
		output.WriteString("\nStored properties:\n")
		for _, property := range entry.Stored {
			_, _ = fmt.Fprintf(&output, "  %s = %s (%s on %s)\n", strings.TrimPrefix(property.Name, policy.Namespace), property.Value, property.Source, property.Dataset)
		}
	}

	var blocked []string
	for i, name := range blockedNames(entry) {
		blocked = append(blocked, name+": "+entry.Blocked[i].Reason)
	}
	writeMessages(&output, "Blocked targets", blocked)
	writeMessages(&output, "Warnings", entry.Policy.Warnings)
	writeMessages(&output, "Errors", entry.Policy.Errors)
	_, err := io.WriteString(writer, output.String())
	return err
}

func activationStatus(entry discovery.Entry) string {
	if !entry.Inspected {
		return "inactive (not locally configured)"
	}
	if entry.Policy.Enabled {
		return "active"
	}
	return "inactive"
}

func actorRoot(entry discovery.Entry) string {
	switch {
	case entry.CoveredBy != "" && entry.Member:
		return "member of " + entry.CoveredBy
	case entry.CoveredBy != "":
		return "excluded from " + entry.CoveredBy
	case len(entry.Members) > 0:
		return "this dataset"
	}
	return "none"
}

// replicaOf describes what marks a dataset as a replica.
func replicaOf(replica discovery.Replica) string {
	var marks []string
	switch {
	case replica.Source != "":
		marks = append(marks, "of "+replica.Source+" on local destination "+replica.Destination)
	case replica.Destination != "":
		marks = append(marks, "beneath replication root "+replica.Destination)
	}
	if replica.Lineage != "" {
		marks = append(marks, "received lineage "+replica.Lineage)
	}
	return strings.Join(marks, "; ")
}

func validStatus(effective policy.Effective) string {
	if effective.Valid() {
		return "valid"
	}
	return "invalid"
}

func encryptionRoot(root string) string {
	if root == "" || root == "-" {
		return "none"
	}
	return root
}

func listOrNone(values []string) string {
	if len(values) == 0 {
		return "none"
	}
	return strings.Join(values, ", ")
}

func formatFlags(flags policy.Flags) string {
	values := make([]string, 0, 6)
	for _, candidate := range []struct {
		name    string
		enabled bool
	}{
		{"large blocks", flags.LargeBlocks},
		{"compressed", flags.Compressed},
		{"embedded data", flags.EmbeddedData},
		{"raw", flags.Raw},
		{"properties", flags.Props},
		{"recursive replication", flags.Replicate},
	} {
		if candidate.enabled {
			values = append(values, candidate.name)
		}
	}
	return listOrNone(values)
}

func writeStringMap(output *strings.Builder, heading string, values map[string]string) {
	if len(values) == 0 {
		return
	}
	output.WriteString("\n" + heading + ":\n")
	for _, key := range slices.Sorted(maps.Keys(values)) {
		_, _ = fmt.Fprintf(output, "  %s = %s\n", key, values[key])
	}
}

func writeList(output *strings.Builder, heading string, values []string) {
	if len(values) == 0 {
		return
	}
	output.WriteString("\n" + heading + ":\n")
	for _, value := range values {
		_, _ = fmt.Fprintf(output, "  %s\n", value)
	}
}

func writeMessages(output *strings.Builder, heading string, values []string) {
	if len(values) == 0 {
		return
	}
	output.WriteString("\n" + heading + ":\n")
	for _, value := range values {
		_, _ = fmt.Fprintf(output, "  - %s\n", value)
	}
}
