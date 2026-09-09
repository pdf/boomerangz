package zfs

import (
	"bufio"
	"context"
	"fmt"
	"slices"
	"strings"
)

type effectiveIdentity struct {
	name   string
	groups map[string]bool
	root   bool
}

type permissionSection uint8

const (
	permissionNone permissionSection = iota
	permissionSets
	permissionCreate
	permissionLocal
	permissionDescendent
	permissionLocalDescendent
)

type permissionGrant struct {
	kind        string
	name        string
	permissions []string
	section     permissionSection
}

type permissionBlock struct {
	setpoint string
	sets     map[string][]string
	grants   []permissionGrant
}

// CheckPermissions verifies that the credential actually executing ZFS at this
// endpoint has every requested delegated permission. Root is authorized by the
// kernel independently of delegation entries.
func (d *Direct) CheckPermissions(ctx context.Context, dataset string, required []string) error {
	if err := validateDataset(dataset); err != nil {
		return err
	}
	required = slices.Clone(required)
	slices.Sort(required)
	required = slices.Compact(required)
	for _, permission := range required {
		if !validPermission(permission) {
			return fmt.Errorf("invalid ZFS permission %q", permission)
		}
	}
	if len(required) == 0 {
		return nil
	}
	identity, err := d.effectiveIdentity(ctx)
	if err != nil {
		return err
	}
	if identity.root {
		return nil
	}
	output, err := d.runner.Run(ctx, "allow", dataset)
	if err != nil {
		return fmt.Errorf("inspect delegated ZFS permissions on %s: %w", dataset, err)
	}
	blocks, err := parsePermissionBlocks(output)
	if err != nil {
		return fmt.Errorf("inspect delegated ZFS permissions on %s: %w", dataset, err)
	}
	granted := effectivePermissions(blocks, dataset, identity)
	missing := slices.DeleteFunc(required, func(permission string) bool { return granted[permission] })
	if len(missing) > 0 {
		return fmt.Errorf("effective account %s lacks delegated ZFS permissions on %s: %s", identity.name, dataset, strings.Join(missing, ","))
	}
	return nil
}

func validPermission(permission string) bool {
	if permission == "" || strings.HasPrefix(permission, "-") {
		return false
	}
	return !strings.ContainsFunc(permission, func(r rune) bool {
		return r == ',' || r == '=' || r == '/' || r == '@' || r <= ' '
	})
}

func (d *Direct) effectiveIdentity(ctx context.Context) (effectiveIdentity, error) {
	if d.identityRunner == nil {
		return effectiveIdentity{}, fmt.Errorf("effective identity query is unavailable")
	}
	uid, err := d.identityRunner.Run(ctx, "-u")
	if err != nil {
		return effectiveIdentity{}, fmt.Errorf("inspect effective user ID: %w", err)
	}
	if strings.TrimSpace(string(uid)) == "0" {
		return effectiveIdentity{name: "root", groups: make(map[string]bool), root: true}, nil
	}
	name, err := d.identityRunner.Run(ctx, "-un")
	if err != nil {
		return effectiveIdentity{}, fmt.Errorf("inspect effective user name: %w", err)
	}
	groups, err := d.identityRunner.Run(ctx, "-Gn")
	if err != nil {
		return effectiveIdentity{}, fmt.Errorf("inspect effective groups: %w", err)
	}
	result := effectiveIdentity{name: strings.TrimSpace(string(name)), groups: make(map[string]bool)}
	if result.name == "" || strings.ContainsAny(result.name, "\r\n\t ") {
		return effectiveIdentity{}, fmt.Errorf("effective user name is invalid")
	}
	for _, group := range strings.Fields(string(groups)) {
		result.groups[group] = true
	}
	return result, nil
}

func parsePermissionBlocks(output []byte) ([]permissionBlock, error) {
	const header = "---- Permissions on "
	var blocks []permissionBlock
	var current *permissionBlock
	section := permissionNone
	scanner := bufio.NewScanner(strings.NewReader(string(output)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, header) {
			name := strings.TrimSpace(strings.TrimRight(strings.TrimPrefix(line, header), "-"))
			if err := validateDataset(name); err != nil {
				return nil, fmt.Errorf("invalid delegation setpoint: %w", err)
			}
			blocks = append(blocks, permissionBlock{setpoint: name, sets: make(map[string][]string)})
			current = &blocks[len(blocks)-1]
			section = permissionNone
			continue
		}
		switch line {
		case "Permission sets:":
			section = permissionSets
			continue
		case "Create time permissions:":
			section = permissionCreate
			continue
		case "Local permissions:":
			section = permissionLocal
			continue
		case "Descendent permissions:":
			section = permissionDescendent
			continue
		case "Local+Descendent permissions:":
			section = permissionLocalDescendent
			continue
		}
		if current == nil || section == permissionNone {
			return nil, fmt.Errorf("unrecognized delegation output line %q", line)
		}
		fields := strings.Fields(line)
		if section == permissionSets {
			if len(fields) != 2 || !strings.HasPrefix(fields[0], "@") {
				return nil, fmt.Errorf("invalid permission-set row %q", line)
			}
			current.sets[fields[0]] = splitPermissions(fields[1])
			continue
		}
		if section == permissionCreate {
			continue
		}
		if len(fields) == 2 && fields[0] == "everyone" {
			current.grants = append(current.grants, permissionGrant{kind: "everyone", permissions: splitPermissions(fields[1]), section: section})
			continue
		}
		if len(fields) != 3 || (fields[0] != "user" && fields[0] != "group") {
			return nil, fmt.Errorf("invalid permission grant row %q", line)
		}
		current.grants = append(current.grants, permissionGrant{kind: fields[0], name: fields[1], permissions: splitPermissions(fields[2]), section: section})
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(blocks) == 0 {
		return nil, fmt.Errorf("delegation output contains no permission setpoint")
	}
	return blocks, nil
}

func splitPermissions(value string) []string {
	return strings.Split(value, ",")
}

func effectivePermissions(blocks []permissionBlock, dataset string, identity effectiveIdentity) map[string]bool {
	result := make(map[string]bool)
	for _, block := range blocks {
		descendant := dataset != block.setpoint && strings.HasPrefix(dataset, block.setpoint+"/")
		if dataset != block.setpoint && !descendant {
			continue
		}
		for _, grant := range block.grants {
			if (grant.section == permissionLocal && descendant) || (grant.section == permissionDescendent && !descendant) {
				continue
			}
			if (grant.kind == "user" && grant.name != identity.name) || (grant.kind == "group" && !identity.groups[grant.name]) {
				continue
			}
			expandPermissions(result, grant.permissions, block.sets, make(map[string]bool))
		}
	}
	// Full receive authority includes the append-only subset used by
	// Boomerangz, although deployments should normally delegate the narrower
	// permission directly.
	if result["receive"] {
		result["receive:append"] = true
	}
	return result
}

func expandPermissions(result map[string]bool, permissions []string, sets map[string][]string, visiting map[string]bool) {
	for _, permission := range permissions {
		if !strings.HasPrefix(permission, "@") {
			result[permission] = true
			continue
		}
		if visiting[permission] {
			continue
		}
		visiting[permission] = true
		expandPermissions(result, sets[permission], sets, visiting)
		delete(visiting, permission)
	}
}
