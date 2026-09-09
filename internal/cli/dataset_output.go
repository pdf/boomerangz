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

func writeDatasetList(writer io.Writer, entries []discovery.Entry) error {
	if len(entries) == 0 {
		_, err := io.WriteString(writer, "No datasets found.\n")
		return err
	}
	var output strings.Builder
	table := tabwriter.NewWriter(&output, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(table, "STATUS\tDATASET")
	invalid := false
	for _, entry := range entries {
		status := datasetStatus(entry)
		invalid = invalid || status == "invalid"
		_, _ = fmt.Fprintf(table, "%s\t%s\n", status, entry.Dataset.Name)
	}
	if err := table.Flush(); err != nil {
		return err
	}
	if invalid {
		output.WriteString("\nInspect invalid datasets with: boomerangz dataset inspect <dataset>\n")
	}
	_, err := io.WriteString(writer, output.String())
	return err
}

func datasetStatus(entry discovery.Entry) string {
	if len(entry.Policy.Errors) > 0 {
		return "invalid"
	}
	if entry.CoveredBy != "" {
		return "covered"
	}
	if entry.Inspected && entry.Policy.Enabled {
		return "active"
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
	line("Covered by", valueOrNone(entry.CoveredBy))
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

func valueOrNone(value string) string {
	if value == "" {
		return "none"
	}
	return value
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
