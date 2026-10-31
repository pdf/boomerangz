package lifecycle

import (
	"strings"
	"testing"

	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/zfs"
)

func TestRootAuthority(t *testing.T) {
	t.Parallel()
	const owner = "11111111-1111-4111-8111-111111111111"
	const lineage = "22222222-2222-4222-8222-222222222222"
	for _, kind := range []string{"local", "foreign", "received", "inherited", "absent", "duplicate", "hidden-lineage", "hidden-owner", "invalid-installation"} {
		t.Run(kind, func(t *testing.T) {
			state := zfs.State{Properties: []zfs.Property{
				{Dataset: "tank/data", Name: OwnerProperty, Value: owner, Source: zfs.SourceLocal},
				{Dataset: "tank/data", Name: LineageProperty, Value: lineage, Source: zfs.SourceLocal},
			}}
			installation := owner
			switch kind {
			case "foreign":
				installation = lineage
			case "received":
				state.Properties[0].Source = zfs.SourceReceived
			case "inherited":
				state.Properties[0].Dataset = "tank"
			case "absent":
				state.Properties = state.Properties[:1]
			case "duplicate":
				state.Properties = append(state.Properties, state.Properties[0])
			case "hidden-lineage":
				state.Received = map[string]map[string]string{"tank/data": {LineageProperty: owner}}
			case "hidden-owner":
				// A promoted replica keeps its source installation's owner.
				state.Received = map[string]map[string]string{"tank/data": {OwnerProperty: lineage, LineageProperty: lineage}}
			case "invalid-installation":
				installation = ""
			}
			actual, err := RootAuthority(state, "tank/data", installation)
			if kind == "local" || kind == "hidden-owner" {
				if err != nil || actual != lineage {
					t.Fatalf("lineage=%s err=%v", actual, err)
				}
			} else if err == nil {
				t.Fatal("unsafe authority accepted")
			} else if kind == "foreign" && !strings.Contains(err.Error(), "dormant foreign") {
				t.Fatal(err)
			}
		})
	}
}

func TestActiveRootRequiresLocalActivation(t *testing.T) {
	t.Parallel()
	parent := policy.Resolve(zfs.Dataset{Name: "tank", Type: zfs.Filesystem, EncryptionRoot: "-"}, nil,
		[]zfs.Property{{Dataset: "tank", Name: policy.Namespace + "enabled", Value: "on", Source: zfs.SourceLocal}}, nil)
	inherited := policy.Resolve(zfs.Dataset{Name: "tank/data", Type: zfs.Filesystem, EncryptionRoot: "-"}, &parent, nil, nil)
	if !inherited.Enabled {
		t.Fatal("fixture: activation did not inherit")
	}
	if err := ActiveRoot(parent, "tank"); err != nil {
		t.Fatalf("local activation refused: %v", err)
	}
	// Inherited activation makes tank/data a member of tank's root. The prefix
	// walk this replaced accepted it, which is what let every descendant act as
	// a peer root.
	if err := ActiveRoot(inherited, "tank/data"); err == nil || !strings.Contains(err.Error(), "member") {
		t.Fatalf("inherited activation accepted: %v", err)
	}
	received := policy.Resolve(zfs.Dataset{Name: "tank/data", Type: zfs.Filesystem, EncryptionRoot: "-"}, nil,
		[]zfs.Property{{Dataset: "tank/data", Name: policy.Namespace + "enabled", Value: "on", Source: zfs.SourceReceived}}, nil)
	if err := ActiveRoot(received, "tank/data"); err == nil {
		t.Fatal("received activation accepted")
	}
}
