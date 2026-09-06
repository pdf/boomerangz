package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/discovery"
	"github.com/pdf/boomerangz/internal/identity"
	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/zfs"
)

type recoveryRoot struct {
	Dataset string `json:"dataset"`
	Owner   string `json:"owner"`
	Lineage string `json:"lineage"`
}

func localMarker(rows []zfs.Property, dataset, name string) (string, error) {
	value := ""
	for _, row := range rows {
		if row.Dataset != dataset || row.Name != name {
			continue
		}
		if row.Source != zfs.SourceLocal {
			continue
		}
		if value != "" {
			return "", fmt.Errorf("%s has ambiguous local %s", dataset, name)
		}
		value = row.Value
	}
	return value, nil
}

func recoveryInventory(ctx context.Context, reader discovery.Reader, executor zfs.Executor, remotes []string) ([]recoveryRoot, map[string]bool, error) {
	datasets, err := reader.ListDatasets(ctx)
	if err != nil {
		return nil, nil, err
	}
	names := make([]string, 0, len(datasets))
	for _, dataset := range datasets {
		names = append(names, dataset.Name)
	}
	scanner, err := discovery.New(reader, discovery.Options{Remotes: remotes})
	if err != nil {
		return nil, nil, err
	}
	generation, err := scanner.Scan(ctx, names)
	if err != nil {
		return nil, nil, err
	}
	allOwners := make(map[string]bool)
	var roots []recoveryRoot
	for _, entry := range generation.Entries() {
		owner, ownerErr := localMarker(entry.Stored, entry.Dataset.Name, lifecycle.OwnerProperty)
		if ownerErr != nil {
			return nil, nil, ownerErr
		}
		if owner != "" {
			if !identity.Valid(owner) {
				return nil, nil, fmt.Errorf("invalid local owner on %s", entry.Dataset.Name)
			}
			allOwners[owner] = true
		}
		if entry.CoveredBy != "" || lifecycle.ActiveRoot(entry.Policy, entry.Dataset.Name) != nil {
			continue
		}
		lineage, lineageErr := localMarker(entry.Stored, entry.Dataset.Name, lifecycle.LineageProperty)
		if lineageErr != nil || owner == "" || !lifecycle.ValidID(lineage) {
			return nil, nil, fmt.Errorf("activated root %s lacks unambiguous local owner and lineage", entry.Dataset.Name)
		}
		state, inspectErr := executor.InspectState(ctx, entry.Dataset.Name, entry.Policy.Send.Replicate)
		if inspectErr != nil {
			return nil, nil, inspectErr
		}
		if _, authorityErr := lifecycle.RootAuthority(state, entry.Dataset.Name, owner); authorityErr != nil {
			return nil, nil, authorityErr
		}
		if evidenceErr := lifecycle.ValidateLineageEvidence(state, entry.Dataset.Name, lineage); evidenceErr != nil {
			return nil, nil, evidenceErr
		}
		roots = append(roots, recoveryRoot{Dataset: entry.Dataset.Name, Owner: owner, Lineage: lineage})
	}
	slices.SortFunc(roots, func(a, b recoveryRoot) int { return strings.Compare(a.Dataset, b.Dataset) })
	return roots, allOwners, nil
}

func runIdentityRecover(ctx context.Context, out io.Writer, cfg config.Config, reader discovery.Reader, executor zfs.Executor, selected string, apply bool) (resultErr error) {
	if selected != "" && !identity.Valid(selected) {
		return fmt.Errorf("--owner must be a canonical UUID")
	}
	if apply {
		lock, err := lifecycleLock(cfg)
		if err != nil {
			return err
		}
		defer func() { resultErr = errors.Join(resultErr, lock.Close()) }()
	}
	if err := (standaloneSafety{socket: cfg.Paths.SocketPath}).Quiescent(ctx, nil); err != nil {
		return err
	}
	current, err := identity.LoadOrCreate(cfg.Paths.IdentityDir)
	if err != nil {
		return err
	}
	remotes := slices.Sorted(maps.Keys(cfg.Remotes))
	roots, allOwners, err := recoveryInventory(ctx, reader, executor, remotes)
	if err != nil {
		return err
	}
	owners := make(map[string]bool)
	for _, root := range roots {
		owners[root.Owner] = true
	}
	if selected == "" {
		if len(owners) != 1 {
			return fmt.Errorf("automatic recovery requires exactly one owner across activated roots")
		}
		for owner := range owners {
			selected = owner
		}
	} else if !owners[selected] {
		return fmt.Errorf("selected owner has no activated root evidence")
	}
	if current == selected {
		return fmt.Errorf("installation identity already matches recovered owner")
	}
	if allOwners[current] {
		return fmt.Errorf("current installation already owns a lineage; recovery refused")
	}
	var selectedRoots []recoveryRoot
	for _, root := range roots {
		if root.Owner == selected {
			selectedRoots = append(selectedRoots, root)
		}
	}
	if apply {
		if err := identity.Recover(cfg.Paths.IdentityDir, current, selected); err != nil {
			return err
		}
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(struct {
		Current   string         `json:"current"`
		Recovered string         `json:"recovered"`
		Roots     []recoveryRoot `json:"roots"`
		Applied   bool           `json:"applied"`
	}{current, selected, selectedRoots, apply})
}
