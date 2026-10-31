package zfs

import (
	"cmp"
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
	// Properties and holds are read for many objects per command rather than
	// one command per object: the work ZFS does is the same, but each command
	// costs a process and, on a remote transport, a round trip. Rows are
	// regrouped per object and applied in the order one query per object
	// would have produced, so the state is the same either way.
	var names, snapshots []string
	for _, object := range state.Objects {
		if object.Type == "bookmark" {
			continue
		}
		names = append(names, object.Name)
		if object.Type == "snapshot" {
			snapshots = append(snapshots, object.Name)
		}
	}
	// The received column is most of what a property query costs: ZFS reads
	// it per object and per property, which made a query for all of them
	// several times slower than the same query without it. Every property is
	// therefore read without it, and the received column is asked only for
	// this namespace, the only one it is read for: the keys the first query
	// listed for some object in the chunk, plus the fixed ownership keys,
	// which also reveal hidden received metadata that all omits. A row of
	// the second query is kept only for a fixed key or for a key the first
	// query listed for that object, so the state is what one query for all
	// with the received column would have produced.
	fixed := []string{propertyNamespace + "state:lineage", propertyNamespace + "state:snapshot"}
	rows := make(map[string][][][]string, len(names))
	add := func(query int, f []string) {
		if rows[f[0]] == nil {
			rows[f[0]] = make([][][]string, 2)
		}
		rows[f[0]][query] = append(rows[f[0]][query], f)
	}
	for _, chunk := range d.inspectionChunks(names) {
		listed := make(map[string]bool)
		keys := slices.Clone(fixed)
		err := d.readRows(ctx, chunk, 4, "property outside exact object query", func(f []string) {
			add(0, []string{f[0], f[1], f[2], "-", f[3]})
			if strings.HasPrefix(f[1], propertyNamespace) {
				listed[f[0]+"\x00"+f[1]] = true
				keys = append(keys, f[1])
			}
		}, append([]string{"get", "-H", "-p", "-o", "name,property,value,source", "all"}, chunk...)...)
		if err != nil {
			return State{}, err
		}
		slices.Sort(keys)
		keys = slices.Compact(keys)
		err = d.readRows(ctx, chunk, 5, "property outside exact object query", func(f []string) {
			if slices.Contains(fixed, f[1]) || listed[f[0]+"\x00"+f[1]] {
				add(1, f)
			}
		}, append([]string{"get", "-H", "-p", "-o", "name,property,value,received,source", strings.Join(keys, ",")}, chunk...)...)
		if err != nil {
			return State{}, err
		}
	}
	for _, name := range names {
		for _, query := range rows[name] {
			for _, f := range query {
				if f[1] == "receive_resume_token" && f[2] != "-" && f[2] != "" {
					state.ResumeTokens[f[0]] = f[2]
				}
				if f[1] == "clones" && f[2] != "-" && f[2] != "" {
					state.Clones[f[0]] = strings.Split(f[2], ",")
				}
				if !strings.HasPrefix(f[1], propertyNamespace) {
					continue
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
			}
		}
	}
	for _, chunk := range d.inspectionChunks(snapshots) {
		err := d.readRows(ctx, chunk, 3, "hold outside queried snapshot", func(f []string) {
			state.Holds[f[0]] = append(state.Holds[f[0]], f[1])
		}, append([]string{"holds", "-H"}, chunk...)...)
		if err != nil {
			return State{}, err
		}
	}
	for _, snapshot := range snapshots {
		slices.Sort(state.Holds[snapshot])
	}
	// ZFS lists an object's user properties, and a snapshot's clones, in no
	// promised order, and the order can change when one is removed. Callers
	// compare inspections to detect change, so the state is canonical.
	slices.SortFunc(state.Properties, compareProperties)
	for _, clones := range state.Clones {
		slices.Sort(clones)
	}
	return state, nil
}

// inspectionBatch bounds how many objects one property or hold query names,
// keeping each command's arguments, and its output, well inside what a remote
// transport accepts: a property query returns a few KB per object.
const inspectionBatch = 256

func (d *Direct) inspectionChunks(names []string) [][]string {
	size := inspectionBatch
	if d.batch > 0 {
		size = d.batch
	}
	var chunks [][]string
	for start := 0; start < len(names); start += size {
		chunks = append(chunks, names[start:min(start+size, len(names))])
	}
	return chunks
}

// readRows runs one query naming the objects in chunk and passes each row to
// row. A row for any other object is refused with outside. An object that
// disappeared since it was listed is reported as an inventory change.
func (d *Direct) readRows(ctx context.Context, chunk []string, columns int, outside string, row func([]string), args ...string) error {
	out, err := d.runner.Run(ctx, args...)
	if err != nil {
		if objectDisappeared(err) {
			return &InventoryChangedError{Object: disappearedObject(err, chunk), Err: err}
		}
		return err
	}
	return parseTable(out, columns, func(f []string) error {
		if !slices.Contains(chunk, f[0]) {
			return fmt.Errorf("%s", outside)
		}
		row(slices.Clone(f))
		return nil
	})
}

// disappearedObject names the object a ZFS error says it could not open, or the
// whole query when it names none of them.
func disappearedObject(err error, chunk []string) string {
	message := err.Error()
	for _, name := range chunk {
		if strings.Contains(message, "cannot open '"+name+"'") {
			return name
		}
	}
	return strings.Join(chunk, ", ")
}

// compareProperties orders properties canonically: by dataset, then name,
// source and value.
func compareProperties(a, b Property) int {
	return cmp.Or(strings.Compare(a.Dataset, b.Dataset), strings.Compare(a.Name, b.Name), strings.Compare(string(a.Source), string(b.Source)), strings.Compare(a.Value, b.Value))
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
// mounted automatically, marked as a placeholder in the same operation. It
// never creates parents implicitly.
func (d *Direct) CreateReceiveParent(ctx context.Context, dataset string) error {
	if err := validateDataset(dataset); err != nil {
		return err
	}
	_, err := d.runner.Run(ctx, "create", "-o", "canmount=noauto", "-o", PlaceholderProperty+"=on", dataset)
	return err
}

// RenameDataset moves one exact dataset, with its descendants and snapshots,
// to another name in the same pool. It never creates parents implicitly.
func (d *Direct) RenameDataset(ctx context.Context, from, to string) error {
	if err := validateDataset(from); err != nil {
		return err
	}
	if err := validateDataset(to); err != nil {
		return err
	}
	fromPool, _, _ := strings.Cut(from, "/")
	toPool, _, _ := strings.Cut(to, "/")
	if fromPool != toPool || from == fromPool || to == toPool || from == to || strings.HasPrefix(to, from+"/") {
		return fmt.Errorf("cannot rename %s to %s", from, to)
	}
	_, err := d.runner.Run(ctx, "rename", from, to)
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
		if len(properties[key]) > MaxUserPropertyValue {
			return fmt.Errorf("property %s value is %d bytes, over the ZFS limit of %d", key, len(properties[key]), MaxUserPropertyValue)
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

// MaxUserPropertyValue is the longest user property value ZFS stores. It
// refuses a value of ZAP_MAXVALUELEN (8192) bytes or more with E2BIG.
const MaxUserPropertyValue = 8191

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

// MergeStates combines inspections of distinct datasets into one state. A
// shallow inspection lists the dataset's direct children too, so an object or
// property two inspections both report is kept once.
func MergeStates(states ...State) State {
	var merged State
	for _, state := range states {
		for _, object := range state.Objects {
			if !slices.Contains(merged.Objects, object) {
				merged.Objects = append(merged.Objects, object)
			}
		}
		for _, property := range state.Properties {
			if !slices.Contains(merged.Properties, property) {
				merged.Properties = append(merged.Properties, property)
			}
		}
		for name, value := range state.Received {
			if merged.Received == nil {
				merged.Received = make(map[string]map[string]string)
			}
			merged.Received[name] = value
		}
		for name, value := range state.ResumeTokens {
			if merged.ResumeTokens == nil {
				merged.ResumeTokens = make(map[string]string)
			}
			merged.ResumeTokens[name] = value
		}
		for name, value := range state.Clones {
			if merged.Clones == nil {
				merged.Clones = make(map[string][]string)
			}
			merged.Clones[name] = value
		}
		for name, value := range state.Holds {
			if merged.Holds == nil {
				merged.Holds = make(map[string][]string)
			}
			merged.Holds[name] = value
		}
	}
	return merged
}
