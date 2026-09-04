package zfs

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"sort"
	"strings"
)

const propertyNamespace = "org.boomerangz:"

type commandRunner interface {
	Run(context.Context, ...string) ([]byte, error)
}

type execRunner struct{ path string }

func (r execRunner) Run(ctx context.Context, args ...string) ([]byte, error) {
	output, err := exec.CommandContext(ctx, r.path, args...).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("zfs %s: %w: %s", args[0], err, strings.TrimSpace(string(output)))
	}
	return output, nil
}

// Direct executes typed operations using a locally installed zfs binary.
type Direct struct{ runner commandRunner }

// NewDirect creates a direct executor for an explicit zfs executable path.
func NewDirect(path string) (*Direct, error) {
	if path == "" {
		return nil, errors.New("zfs executable path is required")
	}
	return &Direct{runner: execRunner{path: path}}, nil
}

// ListDatasets returns the global sparse filesystem and volume inventory.
func (d *Direct) ListDatasets(ctx context.Context) ([]Dataset, error) {
	output, err := d.runner.Run(ctx, "list", "-H", "-p", "-t", "filesystem,volume", "-o", "name,type,encryptionroot")
	if err != nil {
		return nil, err
	}
	var datasets []Dataset
	for lineNumber, line := range nonEmptyLines(output) {
		fields := strings.Split(line, "\t")
		if len(fields) != 3 {
			return nil, fmt.Errorf("parse zfs list line %d: expected 3 tab-separated fields, got %d", lineNumber+1, len(fields))
		}
		typeName := DatasetType(fields[1])
		if typeName != Filesystem && typeName != Volume {
			return nil, fmt.Errorf("parse zfs list line %d: unsupported dataset type %q", lineNumber+1, fields[1])
		}
		datasets = append(datasets, Dataset{Name: fields[0], Type: typeName, EncryptionRoot: fields[2]})
	}
	return datasets, nil
}

// GetStoredProperties retrieves local and received boomerangz properties.
func (d *Direct) GetStoredProperties(ctx context.Context, datasets []string) ([]Property, error) {
	if len(datasets) == 0 {
		return nil, nil
	}
	args := []string{"get", "-H", "-p", "-s", "local,received", "-t", "filesystem,volume", "-o", "name,property,value,source", "all"}
	for _, dataset := range datasets {
		if err := validateDataset(dataset); err != nil {
			return nil, err
		}
		args = append(args, dataset)
	}
	output, err := d.runner.Run(ctx, args...)
	if err != nil {
		return nil, err
	}
	var properties []Property
	for lineNumber, line := range nonEmptyLines(output) {
		fields := strings.Split(line, "\t")
		if len(fields) != 4 {
			return nil, fmt.Errorf("parse zfs get line %d: expected 4 tab-separated fields, got %d", lineNumber+1, len(fields))
		}
		if !strings.HasPrefix(fields[1], propertyNamespace) {
			continue
		}
		source := PropertySource(fields[3])
		if source != SourceLocal && source != SourceReceived {
			return nil, fmt.Errorf("parse zfs get line %d: unsupported property source %q", lineNumber+1, fields[3])
		}
		properties = append(properties, Property{Dataset: fields[0], Name: fields[1], Value: fields[2], Source: source})
	}
	return properties, nil
}

// Snapshot creates a snapshot with validated internal properties.
func (d *Direct) Snapshot(ctx context.Context, dataset, snapshot string, recursive bool, properties map[string]string) error {
	if err := validateDataset(dataset); err != nil {
		return err
	}
	if err := validateComponent("snapshot", snapshot); err != nil {
		return err
	}
	args := []string{"snapshot"}
	if recursive {
		args = append(args, "-r")
	}
	keys := make([]string, 0, len(properties))
	for key := range properties {
		if !strings.HasPrefix(key, propertyNamespace) {
			return fmt.Errorf("snapshot property %q is outside %s", key, propertyNamespace)
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		args = append(args, "-o", key+"="+properties[key])
	}
	_, err := d.runner.Run(ctx, append(args, dataset+"@"+snapshot)...)
	return err
}

// DestroySnapshot destroys one validated snapshot without recursive flags.
func (d *Direct) DestroySnapshot(ctx context.Context, snapshot string) error {
	if err := validateSnapshot(snapshot); err != nil {
		return err
	}
	_, err := d.runner.Run(ctx, "destroy", snapshot)
	return err
}

// Bookmark creates a ZFS bookmark from a snapshot.
func (d *Direct) Bookmark(ctx context.Context, snapshot, bookmark string) error {
	if err := validateSnapshot(snapshot); err != nil {
		return err
	}
	if err := validateBookmark(bookmark); err != nil {
		return err
	}
	_, err := d.runner.Run(ctx, "bookmark", snapshot, bookmark)
	return err
}

// DestroyBookmark destroys one validated bookmark.
func (d *Direct) DestroyBookmark(ctx context.Context, bookmark string) error {
	if err := validateBookmark(bookmark); err != nil {
		return err
	}
	_, err := d.runner.Run(ctx, "destroy", bookmark)
	return err
}

// Hold applies a user hold to one snapshot.
func (d *Direct) Hold(ctx context.Context, tag, snapshot string) error {
	if err := validateComponent("hold tag", tag); err != nil {
		return err
	}
	if err := validateSnapshot(snapshot); err != nil {
		return err
	}
	_, err := d.runner.Run(ctx, "hold", tag, snapshot)
	return err
}

// Release removes a user hold from one snapshot.
func (d *Direct) Release(ctx context.Context, tag, snapshot string) error {
	if err := validateComponent("hold tag", tag); err != nil {
		return err
	}
	if err := validateSnapshot(snapshot); err != nil {
		return err
	}
	_, err := d.runner.Run(ctx, "release", tag, snapshot)
	return err
}

func nonEmptyLines(output []byte) []string {
	trimmed := strings.TrimSpace(string(output))
	if trimmed == "" {
		return nil
	}
	return strings.Split(trimmed, "\n")
}
