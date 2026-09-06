package zfs

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const propertyNamespace = "org.boomerangz:"

type commandRunner interface {
	Run(context.Context, ...string) ([]byte, error)
}

type execRunner struct{ path string }

func (r execRunner) Run(ctx context.Context, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, r.path, args...)
	output := &boundedOutput{}
	command.Stdout, command.Stderr = output, output
	err := command.Run()
	if err != nil {
		return nil, fmt.Errorf("zfs %s: %w: %s", args[0], err, strings.TrimSpace(output.buffer.String()))
	}
	return output.buffer.Bytes(), nil
}

// Bound each sparse or batched query; exceeding the bound fails the scan instead
// of publishing truncated data or allowing unbounded subprocess output.
type boundedOutput struct{ buffer bytes.Buffer }

func (b *boundedOutput) Write(data []byte) (int, error) {
	if len(data) > 32*1024*1024-b.buffer.Len() {
		return 0, errors.New("zfs output exceeds 32 MiB limit")
	}
	return b.buffer.Write(data)
}

// Direct executes typed operations using a locally installed zfs binary.
type Direct struct {
	runner     commandRunner
	poolRunner commandRunner
}

// NewDirect creates a direct executor for an explicit zfs executable path.
func NewDirect(path string) (*Direct, error) {
	if path == "" {
		return nil, errors.New("zfs executable path is required")
	}
	return &Direct{runner: execRunner{path: path}, poolRunner: execRunner{path: filepath.Join(filepath.Dir(path), "zpool")}}, nil
}

// InspectDatasetIdentity resolves an exact dataset GUID and the containing pool
// GUID using typed, bounded queries. It does not infer identity from names.
func (d *Direct) InspectDatasetIdentity(ctx context.Context, dataset string) (DatasetIdentity, error) {
	var result DatasetIdentity
	if err := validateDataset(dataset); err != nil {
		return result, err
	}
	output, err := d.runner.Run(ctx, "list", "-H", "-p", "-d", "0", "-t", "filesystem,volume", "-o", "name,type,guid", dataset)
	if err != nil {
		return result, err
	}
	err = parseTable(output, 3, func(fields []string) error {
		if result.Name != "" || fields[0] != dataset {
			return fmt.Errorf("unexpected dataset identity row %q", fields[0])
		}
		typeName := DatasetType(fields[1])
		if typeName != Filesystem && typeName != Volume {
			return fmt.Errorf("unsupported dataset type %q", fields[1])
		}
		guid, parseErr := strconv.ParseUint(fields[2], 10, 64)
		if parseErr != nil || guid == 0 {
			return fmt.Errorf("invalid dataset GUID")
		}
		result.Name, result.Type, result.GUID = fields[0], typeName, guid
		return nil
	})
	if err != nil {
		return DatasetIdentity{}, err
	}
	if result.Name == "" {
		return DatasetIdentity{}, fmt.Errorf("dataset identity is missing")
	}
	result.Pool, _, _ = strings.Cut(dataset, "/")
	if d.poolRunner == nil {
		return DatasetIdentity{}, fmt.Errorf("pool identity query is unavailable")
	}
	output, err = d.poolRunner.Run(ctx, "get", "-H", "-p", "-o", "name,property,value", "guid", result.Pool)
	if err != nil {
		return DatasetIdentity{}, err
	}
	err = parseTable(output, 3, func(fields []string) error {
		if fields[0] != result.Pool || fields[1] != "guid" || result.PoolGUID != 0 {
			return fmt.Errorf("unexpected pool identity row")
		}
		guid, parseErr := strconv.ParseUint(fields[2], 10, 64)
		if parseErr != nil || guid == 0 {
			return fmt.Errorf("invalid pool GUID")
		}
		result.PoolGUID = guid
		return nil
	})
	if err != nil {
		return DatasetIdentity{}, err
	}
	if result.PoolGUID == 0 {
		return DatasetIdentity{}, fmt.Errorf("pool identity is missing")
	}
	return result, nil
}

// ListDatasets returns the global sparse filesystem and volume inventory.
func (d *Direct) ListDatasets(ctx context.Context) ([]Dataset, error) {
	output, err := d.runner.Run(ctx, "list", "-H", "-p", "-t", "filesystem,volume", "-o", "name,type,encryptionroot")
	if err != nil {
		return nil, err
	}
	return parseDatasets(output)
}

func parseDatasets(output []byte) ([]Dataset, error) {
	var datasets []Dataset
	err := parseTable(output, 3, func(fields []string) error {
		if err := validateDataset(fields[0]); err != nil {
			return err
		}
		typeName := DatasetType(fields[1])
		if typeName != Filesystem && typeName != Volume {
			return fmt.Errorf("unsupported dataset type %q", fields[1])
		}
		datasets = append(datasets, Dataset{Name: fields[0], Type: typeName, EncryptionRoot: fields[2]})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return datasets, nil
}

// GetActivationProperties retrieves the global sparse activation inventory.
func (d *Direct) GetActivationProperties(ctx context.Context) ([]Property, error) {
	output, err := d.runner.Run(ctx, "get", "-H", "-p", "-s", "local,received", "-t", "filesystem,volume", "-o", "name,property,value,source", propertyNamespace+"enabled")
	if err != nil {
		return nil, err
	}
	return parseProperties(output)
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
	return parseProperties(output)
}

func parseProperties(output []byte) ([]Property, error) {
	var properties []Property
	err := parseTable(output, 4, func(fields []string) error {
		if !strings.HasPrefix(fields[1], propertyNamespace) {
			return nil
		}
		if err := validateDataset(fields[0]); err != nil {
			return err
		}
		source := PropertySource(fields[3])
		if source != SourceLocal && source != SourceReceived {
			return fmt.Errorf("unsupported property source %q", fields[3])
		}
		properties = append(properties, Property{Dataset: fields[0], Name: fields[1], Value: fields[2], Source: source})
		return nil
	})
	if err != nil {
		return nil, err
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

func parseTable(output []byte, columns int, consume func([]string) error) error {
	scanner := bufio.NewScanner(bytes.NewReader(output))
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	line := 0
	for scanner.Scan() {
		line++
		fields := strings.Split(scanner.Text(), "\t")
		if len(fields) != columns {
			return fmt.Errorf("parse zfs output line %d: expected %d fields, got %d", line, columns, len(fields))
		}
		if err := consume(fields); err != nil {
			return fmt.Errorf("parse zfs output line %d: %w", line, err)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("parse zfs output: %w", err)
	}
	return nil
}
