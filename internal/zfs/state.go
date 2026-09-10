package zfs

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

const inspectStateAttempts = 3

// InventoryChangedError reports an object that disappeared after ZFS listed it
// but before its properties or holds could be read. Callers may safely retry the
// complete inventory; no partial state is returned.
type InventoryChangedError struct {
	Object string
	Err    error
}

func (e *InventoryChangedError) Error() string {
	return fmt.Sprintf("zfs inventory changed while inspecting %s: %v", e.Object, e.Err)
}

func (e *InventoryChangedError) Unwrap() error { return e.Err }

// Temporary identifies bounded inventory churn to transfer coordinators.
func (*InventoryChangedError) Temporary() bool { return true }

func objectDisappeared(err error) bool {
	message := err.Error()
	return strings.Contains(message, "cannot open '") && strings.Contains(message, "dataset does not exist")
}

// InspectState inventories one exact dataset or a recursive dataset scope.
// Each subprocess is bounded; a failure never returns a partial state.
func (d *Direct) InspectState(ctx context.Context, dataset string, recursive bool) (State, error) {
	var changed *InventoryChangedError
	for range inspectStateAttempts {
		state, err := d.inspectState(ctx, dataset, recursive)
		if err == nil {
			return state, nil
		}
		if !errors.As(err, &changed) {
			return State{}, err
		}
		if err := ctx.Err(); err != nil {
			return State{}, err
		}
	}
	return State{}, changed
}

func (d *Direct) inspectState(ctx context.Context, dataset string, recursive bool) (State, error) {
	if err := validateDataset(dataset); err != nil {
		return State{}, err
	}
	args := []string{"list", "-H", "-p", "-r"}
	if !recursive {
		args = append(args, "-d", "1")
	}
	args = append(args, "-t", "filesystem,volume,snapshot,bookmark", "-o", "name,type,guid,creation,createtxg", dataset)
	output, err := d.runner.Run(ctx, args...)
	if err != nil {
		return State{}, err
	}
	state := State{Received: make(map[string]map[string]string), ResumeTokens: make(map[string]string), Clones: make(map[string][]string), Holds: make(map[string][]string)}
	seen := make(map[string]bool)
	err = parseTable(output, 5, func(fields []string) error {
		base := strings.FieldsFunc(fields[0], func(r rune) bool { return r == '@' || r == '#' })
		if len(base) == 0 || (base[0] != dataset && !strings.HasPrefix(base[0], dataset+"/")) {
			return fmt.Errorf("object outside query scope: %q", fields[0])
		}
		if !recursive && base[0] != dataset {
			return nil
		}
		if err := validateObject(fields[0]); err != nil {
			return err
		}
		if seen[fields[0]] {
			return fmt.Errorf("duplicate object %q", fields[0])
		}
		seen[fields[0]] = true
		guid, err := strconv.ParseUint(fields[2], 10, 64)
		if err != nil || guid == 0 {
			return fmt.Errorf("invalid GUID for %s", fields[0])
		}
		created, err := strconv.ParseInt(fields[3], 10, 64)
		if err != nil || created < 0 {
			return fmt.Errorf("invalid creation for %s", fields[0])
		}
		if fields[1] != "filesystem" && fields[1] != "volume" && fields[1] != "snapshot" && fields[1] != "bookmark" {
			return fmt.Errorf("unexpected object type %q", fields[1])
		}
		if (fields[1] == "snapshot") != strings.Contains(fields[0], "@") || (fields[1] == "bookmark") != strings.Contains(fields[0], "#") {
			return fmt.Errorf("object name and type disagree: %q", fields[0])
		}
		txg, err := strconv.ParseUint(fields[4], 10, 64)
		if err != nil || txg == 0 {
			return fmt.Errorf("invalid creation transaction for %s", fields[0])
		}
		state.Objects = append(state.Objects, Object{Name: fields[0], Type: fields[1], GUID: guid, Creation: created, CreateTXG: txg})
		return nil
	})
	if err != nil {
		return State{}, err
	}
	if !seen[dataset] {
		return State{}, fmt.Errorf("dataset missing from inventory")
	}
	slices.SortFunc(state.Objects, func(a, b Object) int { return strings.Compare(a.Name, b.Name) })
	for _, object := range state.Objects {
		if object.Type == "bookmark" {
			continue
		}
		// Fixed ownership keys also reveal hidden received metadata omitted by all.
		for _, keys := range []string{"all", propertyNamespace + "state:lineage," + propertyNamespace + "state:snapshot," + propertyNamespace + "state:created"} {
			out, err := d.runner.Run(ctx, "get", "-H", "-p", "-o", "name,property,value,received,source", keys, object.Name)
			if err != nil {
				if objectDisappeared(err) {
					return State{}, &InventoryChangedError{Object: object.Name, Err: err}
				}
				return State{}, err
			}
			err = parseTable(out, 5, func(f []string) error {
				if f[0] != object.Name {
					return fmt.Errorf("property outside exact object query")
				}
				if f[1] == "receive_resume_token" && f[2] != "-" && f[2] != "" {
					state.ResumeTokens[f[0]] = f[2]
				}
				if f[1] == "clones" && f[2] != "-" && f[2] != "" {
					state.Clones[f[0]] = strings.Split(f[2], ",")
				}
				if !strings.HasPrefix(f[1], propertyNamespace) {
					return nil
				}
				if f[3] != "-" {
					if state.Received[f[0]] == nil {
						state.Received[f[0]] = make(map[string]string)
					}
					state.Received[f[0]][f[1]] = f[3]
				}
				if f[4] == "local" || f[4] == "received" {
					p := Property{Dataset: f[0], Name: f[1], Value: f[2], Source: PropertySource(f[4])}
					if !slices.Contains(state.Properties, p) {
						state.Properties = append(state.Properties, p)
					}
				}
				return nil
			})
			if err != nil {
				return State{}, err
			}
		}
		if object.Type == "snapshot" {
			out, err := d.runner.Run(ctx, "holds", "-H", object.Name)
			if err != nil {
				if objectDisappeared(err) {
					return State{}, &InventoryChangedError{Object: object.Name, Err: err}
				}
				return State{}, err
			}
			err = parseTable(out, 3, func(f []string) error {
				if f[0] != object.Name {
					return fmt.Errorf("hold outside queried snapshot")
				}
				state.Holds[f[0]] = append(state.Holds[f[0]], f[1])
				return nil
			})
			if err != nil {
				return State{}, err
			}
			slices.Sort(state.Holds[object.Name])
		}
	}
	return state, nil
}

func validateObject(name string) error {
	if strings.Contains(name, "@") {
		return validateSnapshot(name)
	}
	if strings.Contains(name, "#") {
		return validateBookmark(name)
	}
	return validateDataset(name)
}

// CreateReceiveParent creates one exact receive container that is never
// mounted automatically. It never creates parents implicitly.
func (d *Direct) CreateReceiveParent(ctx context.Context, dataset string) error {
	if err := validateDataset(dataset); err != nil {
		return err
	}
	_, err := d.runner.Run(ctx, "create", "-o", "canmount=noauto", dataset)
	return err
}

// AbortReceive discards resumable receive state on one exact dataset.
func (d *Direct) AbortReceive(ctx context.Context, dataset string) error {
	if err := validateDataset(dataset); err != nil {
		return err
	}
	_, err := d.runner.Run(ctx, "receive", "-A", dataset)
	return err
}

// DestroyDataset destroys one exact dataset and, when requested, descendants.
func (d *Direct) DestroyDataset(ctx context.Context, dataset string, recursive bool) error {
	if err := validateDataset(dataset); err != nil {
		return err
	}
	args := []string{"destroy"}
	if recursive {
		args = append(args, "-r")
	}
	_, err := d.runner.Run(ctx, append(args, dataset)...)
	return err
}

// SetProperties sets only boomerangz namespace properties on an exact object.
func (d *Direct) SetProperties(ctx context.Context, object string, properties map[string]string) error {
	if err := validateObject(object); err != nil {
		return err
	}
	if len(properties) == 0 {
		return nil
	}
	var keys []string
	for key := range properties {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	args := []string{"set"}
	for _, key := range keys {
		if err := validateUserProperty(key); err != nil {
			return err
		}
		if strings.ContainsRune(properties[key], 0) {
			return fmt.Errorf("property value contains NUL")
		}
		args = append(args, key+"="+properties[key])
	}
	_, err := d.runner.Run(ctx, append(args, object)...)
	return err
}

// InheritProperty removes a local value and masks, but does not erase, a
// received value. It never requests -S, which could restore received policy.
func (d *Direct) InheritProperty(ctx context.Context, object, property string) error {
	if err := validateObject(object); err != nil {
		return err
	}
	if err := validateUserProperty(property); err != nil {
		return err
	}
	_, err := d.runner.Run(ctx, "inherit", property, object)
	return err
}

func validateUserProperty(name string) error {
	if !strings.HasPrefix(name, propertyNamespace) || len(name) == len(propertyNamespace) {
		return fmt.Errorf("property outside boomerangz namespace")
	}
	for _, r := range name {
		valid := r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == ':' || r == '_' || r == '-' || r == '.'
		if !valid {
			return fmt.Errorf("invalid user property %q", name)
		}
	}
	return nil
}
