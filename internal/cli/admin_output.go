package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/migrate"
	"github.com/pdf/boomerangz/internal/transfer"
)

// The preview/apply administrative commands render what an operator reviews
// before --apply: every action with its object and property, every blocker,
// every warning, and what was applied. --json emits the same result as JSON.

// writeJSON emits value as indented JSON, the --json form of every
// preview/apply command.
func writeJSON(writer io.Writer, value any) error {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

// writePreviewNote says that a preview changed nothing.
func writePreviewNote(output *strings.Builder, apply bool) {
	if !apply {
		output.WriteString("\nPreview only; nothing was changed. Apply with --apply.\n")
	}
}

func yesNo(value bool) string {
	if value {
		return "yes"
	}
	return "no"
}

func writeCleanPlans(writer io.Writer, plans []lifecycle.CleanPlan, apply bool) error {
	var output strings.Builder
	if len(plans) == 0 {
		output.WriteString("No datasets selected.\n")
	}
	for i, plan := range plans {
		if i > 0 {
			output.WriteString("\n")
		}
		line := func(label, value string) { _, _ = fmt.Fprintf(&output, "%s: %s\n", label, value) }
		line("Dataset", plan.Dataset)
		line("Options", cleanOptions(plan.Options))
		actions := make([]string, 0, len(plan.Actions))
		for _, action := range plan.Actions {
			actions = append(actions, cleanAction(action))
		}
		writeActions(&output, actions)
		writeMessages(&output, "Blockers", plan.Blockers)
		writeMessages(&output, "Warnings", plan.Warnings)
		_, _ = fmt.Fprintf(&output, "\nApplied: %d of %d actions\n", plan.Applied, len(plan.Actions))
	}
	writePreviewNote(&output, apply)
	_, err := io.WriteString(writer, output.String())
	return err
}

func cleanOptions(options lifecycle.CleanOptions) string {
	var values []string
	if options.Recursive {
		values = append(values, "recursive")
	}
	if options.DestroyOwnedSnapshots {
		values = append(values, "destroy owned snapshots")
	}
	for _, target := range options.ForgetTargets {
		values = append(values, "forget target "+target)
	}
	return listOrNone(values)
}

// cleanAction names one action: the operation, the object it acts on, the
// property or hold tag it concerns, and the GUID it was proven by.
func cleanAction(action lifecycle.CleanAction) string {
	text := action.Operation + " " + action.Object
	if action.Property != "" {
		text += " " + action.Property
	}
	if action.GUID != 0 {
		text += " (guid " + strconv.FormatUint(action.GUID, 10) + ")"
	}
	return text
}

// writeActions lists actions, or says there are none.
func writeActions(output *strings.Builder, actions []string) {
	if len(actions) == 0 {
		output.WriteString("\nActions: none\n")
		return
	}
	output.WriteString("\nActions:\n")
	for _, action := range actions {
		_, _ = fmt.Fprintf(output, "  %s\n", action)
	}
}

func writeAdoption(writer io.Writer, result adoptionResult, apply bool) error {
	var output strings.Builder
	line := func(label, value string) { _, _ = fmt.Fprintf(&output, "%s: %s\n", label, value) }
	line("Dataset", result.Dataset)
	line("Lineage", result.Lineage)
	if result.OldOwner == "" {
		line("Current owner", "none")
	} else {
		line("Current owner", result.OldOwner)
	}
	line("New owner", result.NewOwner)
	line("Snapshot policy", result.Policy.Grid.String())
	line("Local destinations", listOrNone(result.Policy.Local))
	line("Remote destinations", listOrNone(result.Policy.Remote))
	writeTargetInspections(&output, "Local targets", result.LocalTargets)
	writeTargetInspections(&output, "Remote targets", result.RemoteTargets)
	writeMessages(&output, "Blockers", result.Blockers)
	writeMessages(&output, "Warnings", append([]string{result.Warning}, result.Policy.Warnings...))
	_, _ = fmt.Fprintf(&output, "\nApplied: %s\n", yesNo(result.Applied))
	writePreviewNote(&output, apply)
	_, err := io.WriteString(writer, output.String())
	return err
}

// writeTargetInspections lists each target an adoption verified: its status,
// and where it receives.
func writeTargetInspections(output *strings.Builder, heading string, targets []transfer.LocalTargetInspection) {
	if len(targets) == 0 {
		return
	}
	output.WriteString("\n" + heading + ":\n")
	for _, target := range targets {
		var details []string
		add := func(label, value string) {
			if value != "" {
				details = append(details, label+" "+value)
			}
		}
		add("endpoint", target.CanonicalEndpoint)
		add("mode", target.EndpointMode)
		add("mapped to", target.MappedDataset)
		switch {
		case target.Stored != nil:
			details = append(details, "binding stored")
		case target.Resolved != nil:
			details = append(details, "binding not yet stored")
		}
		_, _ = fmt.Fprintf(output, "  %s: %s", target.ConfiguredName, target.Status)
		if len(details) > 0 {
			_, _ = fmt.Fprintf(output, " (%s)", strings.Join(details, "; "))
		}
		output.WriteString("\n")
	}
}

func writeReseedPlan(writer io.Writer, plan transfer.ReseedPlan, apply bool) error {
	if plan.Dataset == "" {
		// The target could not be planned; the error says why.
		return nil
	}
	var output strings.Builder
	line := func(label, value string) { _, _ = fmt.Fprintf(&output, "%s: %s\n", label, value) }
	line("Dataset", plan.Dataset)
	line("Target", plan.Target)
	binding := plan.Binding
	line("Mapped replica", binding.MappedDataset)
	line("Destination pool", fmt.Sprintf("%s (guid %d)", binding.Pool, binding.PoolGUID))
	line("Binding anchor", fmt.Sprintf("%s (guid %d)", binding.Anchor, binding.AnchorGUID))
	line("Binding stored", yesNo(plan.BindingStored))
	line("Receive anchor", plan.ReceiveAnchor)
	line("Replica exists", yesNo(plan.DestinationExists))
	if plan.SetAside != "" {
		line("Set-aside placeholder", plan.SetAside)
	}
	if plan.Suspended {
		line("Target suspended", "yes")
	}
	writeList(&output, "Destination objects to destroy", plan.DestinationObjects)
	writeList(&output, "Interrupted receives to discard", plan.ResumeDatasets)
	var references []string
	for _, reference := range plan.References {
		text := fmt.Sprintf("%s: snapshot %s of lineage %s, created %s", reference.Dataset, reference.Metadata.Snapshot, reference.Metadata.Lineage, reference.Metadata.Created.Format("2006-01-02T15:04:05Z07:00"))
		if reference.Recursive {
			text += ", recursive"
		}
		references = append(references, text)
	}
	writeList(&output, "Source proofs to release", references)
	writeMessages(&output, "Warnings", []string{plan.Warning})
	_, _ = fmt.Fprintf(&output, "\nApplied: %s\n", yesNo(plan.Applied))
	writePreviewNote(&output, apply)
	_, err := io.WriteString(writer, output.String())
	return err
}

func writeAbortPlan(writer io.Writer, plan transfer.AbortPlan, apply bool) error {
	if plan.Dataset == "" {
		// The target could not be planned; the error says why.
		return nil
	}
	var output strings.Builder
	line := func(label, value string) { _, _ = fmt.Fprintf(&output, "%s: %s\n", label, value) }
	line("Dataset", plan.Dataset)
	line("Target", plan.Target)
	if len(plan.Receives) == 0 {
		output.WriteString("\nInterrupted receives to discard: none\n")
	}
	writeList(&output, "Interrupted receives to discard", plan.Receives)
	writeMessages(&output, "Warnings", []string{plan.Warning})
	_, _ = fmt.Fprintf(&output, "\nApplied: %s\n", yesNo(plan.Applied))
	writePreviewNote(&output, apply)
	_, err := io.WriteString(writer, output.String())
	return err
}

func writeRecovery(writer io.Writer, result recoveryResult, apply bool) error {
	var output strings.Builder
	line := func(label, value string) { _, _ = fmt.Fprintf(&output, "%s: %s\n", label, value) }
	line("Current identity", result.Current)
	line("Recovered identity", result.Recovered)
	roots := make([]string, 0, len(result.Roots))
	for _, root := range result.Roots {
		roots = append(roots, fmt.Sprintf("%s (lineage %s)", root.Dataset, root.Lineage))
	}
	writeList(&output, "Roots owned by the recovered identity", roots)
	_, _ = fmt.Fprintf(&output, "\nApplied: %s\n", yesNo(result.Applied))
	writePreviewNote(&output, apply)
	_, err := io.WriteString(writer, output.String())
	return err
}

func writeMigrationPlans(writer io.Writer, plans []migrate.Plan, apply bool) error {
	var output strings.Builder
	if len(plans) == 0 {
		output.WriteString("No datasets keep boomerangz state.\n")
	}
	for i, plan := range plans {
		if i > 0 {
			output.WriteString("\n")
		}
		line := func(label, value string) { _, _ = fmt.Fprintf(&output, "%s: %s\n", label, value) }
		line("Dataset", plan.Dataset)
		line("Status", plan.Status)
		line("Format", migrationFormat(plan))
		if len(plan.Actions) > 0 {
			output.WriteString("\nActions:\n")
			for _, action := range plan.Actions {
				_, _ = fmt.Fprintf(&output, "  set %s\n", action.Property)
				if action.Previous != "" {
					_, _ = fmt.Fprintf(&output, "    from: %s\n", action.Previous)
				}
				_, _ = fmt.Fprintf(&output, "    to:   %s\n", action.Value)
			}
		}
		writeMessages(&output, "Blockers", plan.Blockers)
		if len(plan.Actions) > 0 {
			_, _ = fmt.Fprintf(&output, "\nApplied: %d of %d actions\n", plan.Applied, len(plan.Actions))
		}
	}
	writePreviewNote(&output, apply)
	_, err := io.WriteString(writer, output.String())
	return err
}

func migrationFormat(plan migrate.Plan) string {
	switch {
	case plan.Status == migrate.StatusUnmanaged:
		return "none; keeps no state"
	case plan.From == 0:
		return "unreadable"
	case plan.From == plan.To:
		return fmt.Sprintf("v%d (current)", plan.To)
	case plan.From > plan.To:
		return fmt.Sprintf("v%d, newer than this build's v%d", plan.From, plan.To)
	}
	return fmt.Sprintf("v%d to v%d", plan.From, plan.To)
}
