package lifecycle

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/zfs"
)

const inactiveMarkerVersion = 1

// InactiveProperty records when an owned source root first became inactive.
const InactiveProperty = policy.StateNamespace + "inactive"

// InactiveMarker binds a grace-period timestamp to the exact source authority.
type InactiveMarker struct {
	Version     int    `json:"version"`
	Since       string `json:"since"`
	DatasetGUID uint64 `json:"dataset_guid"`
	Lineage     string `json:"lineage"`
	Owner       string `json:"owner"`
}

// InactivePlan is the durable transition and retirement-eligibility view for
// one exact independently managed source root.
type InactivePlan struct {
	Dataset   string          `json:"dataset"`
	Active    bool            `json:"active"`
	Marker    *InactiveMarker `json:"marker,omitempty"`
	Action    string          `json:"action,omitempty"`
	Deadline  *time.Time      `json:"deadline,omitempty"`
	Due       bool            `json:"due"`
	Automatic bool            `json:"automatic"`
	Applied   bool            `json:"applied"`
}

// RetirementPlan combines grace-period eligibility with the exact clean plan
// that may run once the source is safely retireable.
type RetirementPlan struct {
	Inactive InactivePlan `json:"inactive"`
	Eligible bool         `json:"eligible"`
	Clean    CleanPlan    `json:"clean"`
}

type inactiveBackend interface {
	InheritProperty(context.Context, string, string) error
}

func exactDatasetGUID(state zfs.State, dataset string) (uint64, error) {
	var guid uint64
	for _, object := range state.Objects {
		if object.Name != dataset || (object.Type != "filesystem" && object.Type != "volume") {
			continue
		}
		if guid != 0 || object.GUID == 0 {
			return 0, fmt.Errorf("inactive source identity is ambiguous or incomplete")
		}
		guid = object.GUID
	}
	if guid == 0 {
		return 0, fmt.Errorf("inactive source dataset is missing")
	}
	return guid, nil
}

func decodeInactiveMarker(state zfs.State, dataset string) (*InactiveMarker, error) {
	var marker *InactiveMarker
	for _, property := range state.Properties {
		if property.Dataset != dataset || property.Name != InactiveProperty {
			continue
		}
		if marker != nil || property.Source != zfs.SourceLocal {
			return nil, fmt.Errorf("inactive marker must be unambiguous and explicitly local")
		}
		var decoded InactiveMarker
		decoder := json.NewDecoder(bytes.NewBufferString(property.Value))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&decoded); err != nil {
			return nil, fmt.Errorf("invalid inactive marker")
		}
		if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("invalid inactive marker")
		}
		marker = &decoded
	}
	if state.Received[dataset][InactiveProperty] != "" {
		return nil, fmt.Errorf("received inactive marker is not authority")
	}
	return marker, nil
}

func validateInactiveMarker(marker InactiveMarker, guid uint64, lineage, installation string) (time.Time, error) {
	since, err := time.Parse(time.RFC3339Nano, marker.Since)
	if err != nil || marker.Version != inactiveMarkerVersion || marker.DatasetGUID != guid || marker.Lineage != lineage || marker.Owner != installation || marker.Since != since.UTC().Format(time.RFC3339Nano) {
		return time.Time{}, fmt.Errorf("inactive marker does not match current source authority")
	}
	return since.UTC(), nil
}

func inactivePlan(state zfs.State, dataset, installation string, active bool, observedAt time.Time, grace time.Duration) (InactivePlan, error) {
	plan := InactivePlan{Dataset: dataset, Active: active, Automatic: grace > 0}
	if err := zfs.ValidateDataset(dataset); err != nil {
		return plan, err
	}
	if observedAt.IsZero() || observedAt.Year() < 1 || observedAt.Year() > 9999 || grace < 0 {
		return plan, fmt.Errorf("valid inactive observation time and nonnegative grace period required")
	}
	marker, err := decodeInactiveMarker(state, dataset)
	if err != nil {
		return plan, err
	}
	if marker == nil && active {
		return plan, nil
	}
	lineage, err := RootAuthority(state, dataset, installation)
	if err != nil {
		return plan, err
	}
	guid, err := exactDatasetGUID(state, dataset)
	if err != nil {
		return plan, err
	}
	if marker == nil {
		created := InactiveMarker{Version: inactiveMarkerVersion, Since: observedAt.UTC().Format(time.RFC3339Nano), DatasetGUID: guid, Lineage: lineage, Owner: installation}
		plan.Marker, plan.Action = &created, "set-inactive-marker"
		if grace > 0 {
			deadline := observedAt.UTC().Add(grace)
			plan.Deadline = &deadline
		}
		return plan, nil
	}
	since, err := validateInactiveMarker(*marker, guid, lineage, installation)
	if err != nil {
		return plan, err
	}
	plan.Marker = marker
	if active {
		plan.Action = "clear-inactive-marker"
		return plan, nil
	}
	if grace > 0 {
		deadline := since.Add(grace)
		plan.Deadline = &deadline
		plan.Due = !observedAt.UTC().Before(deadline)
	}
	return plan, nil
}

func locallyActive(properties []zfs.Property, dataset string) (bool, error) {
	nearest := ""
	value := "off"
	seen := make(map[string]bool)
	for _, property := range properties {
		if property.Name != policy.Namespace+"enabled" || property.Source != zfs.SourceLocal || (property.Dataset != dataset && !strings.HasPrefix(dataset, property.Dataset+"/")) {
			continue
		}
		if seen[property.Dataset] || (property.Value != "on" && property.Value != "off") {
			return false, fmt.Errorf("inactive source activation is ambiguous or invalid")
		}
		seen[property.Dataset] = true
		if len(property.Dataset) > len(nearest) {
			nearest, value = property.Dataset, property.Value
		}
	}
	return value == "on", nil
}

// ReconcileInactive previews or applies the durable marker transition. The
// daemon supplies active from a complete immutable policy generation.
func (s *Service) ReconcileInactive(ctx context.Context, dataset string, active bool, observedAt time.Time, grace time.Duration, apply bool) (InactivePlan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.backend.InspectState(ctx, dataset, false)
	if err != nil {
		return InactivePlan{Dataset: dataset, Active: active}, err
	}
	plan, err := inactivePlan(state, dataset, s.installation, active, observedAt, grace)
	if err != nil || !apply || plan.Action == "" {
		return plan, err
	}
	if err := s.unchanged(ctx, dataset, false, state); err != nil {
		return plan, err
	}
	switch plan.Action {
	case "set-inactive-marker":
		value, encodeErr := json.Marshal(plan.Marker)
		if encodeErr != nil {
			return plan, encodeErr
		}
		err = s.backend.SetProperties(ctx, dataset, map[string]string{InactiveProperty: string(value)})
	case "clear-inactive-marker":
		backend, ok := s.backend.(inactiveBackend)
		if !ok {
			return plan, fmt.Errorf("inactive marker clearing is unavailable")
		}
		err = backend.InheritProperty(ctx, dataset, InactiveProperty)
	default:
		return plan, fmt.Errorf("unknown inactive transition")
	}
	if err != nil {
		return plan, err
	}
	after, err := s.backend.InspectState(ctx, dataset, false)
	if err != nil {
		return plan, err
	}
	verified, err := inactivePlan(after, dataset, s.installation, active, observedAt, grace)
	if err != nil || verified.Action != "" || !reflect.DeepEqual(datasetObjects(state), datasetObjects(after)) {
		return plan, fmt.Errorf("inactive marker transition could not be verified")
	}
	plan.Applied = true
	return plan, nil
}

// Retire previews or applies the ownership-safe local retirement plan after an
// inactive marker's grace period. Phase 6 supplies scheduling and target-side
// retirement coordination through CleanSafety.
func (s *Service) Retire(ctx context.Context, dataset string, recursive bool, observedAt time.Time, grace time.Duration, apply bool, safety CleanSafety) (RetirementPlan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := RetirementPlan{}
	backend, ok := s.backend.(cleanBackend)
	if !ok {
		return result, fmt.Errorf("retirement operations unavailable")
	}
	state, err := s.backend.InspectState(ctx, dataset, recursive)
	if err != nil {
		return result, err
	}
	activation, err := backend.GetActivationProperties(ctx)
	if err != nil {
		return result, err
	}
	active, err := locallyActive(activation, dataset)
	if err != nil {
		return result, err
	}
	result.Inactive, err = inactivePlan(state, dataset, s.installation, active, observedAt, grace)
	if err != nil {
		return result, err
	}
	result.Eligible = !active && result.Inactive.Action == "" && result.Inactive.Automatic && result.Inactive.Due
	if !result.Eligible {
		return result, nil
	}
	result.Clean, err = s.clean(ctx, dataset, CleanOptions{Recursive: recursive, DestroyOwnedSnapshots: true}, apply, safety, cleanMode{retirement: true, now: observedAt, grace: grace})
	return result, err
}
