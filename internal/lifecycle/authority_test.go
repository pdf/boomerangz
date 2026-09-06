package lifecycle

import (
	"strings"
	"testing"

	"github.com/pdf/boomerangz/internal/zfs"
)

func TestRootAuthority(t *testing.T) {
	t.Parallel()
	const owner = "11111111-1111-4111-8111-111111111111"
	const lineage = "22222222-2222-4222-8222-222222222222"
	for _, kind := range []string{"local", "foreign", "received", "inherited", "absent", "duplicate", "hidden-conflict", "invalid-installation"} {
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
			case "hidden-conflict":
				state.Received = map[string]map[string]string{"tank/data": {OwnerProperty: lineage}}
			case "invalid-installation":
				installation = ""
			}
			actual, err := RootAuthority(state, "tank/data", installation)
			if kind == "local" {
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
