package transfer

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/zfs"
)

const fixtureLineage = "11111111-1111-4111-8111-111111111111"
const fixtureInstallation = "22222222-2222-4222-8222-222222222222"

func testFixture(t *testing.T) (Request, View) {
	t.Helper()
	source := zfs.Dataset{Name: "tank/data", Type: zfs.Filesystem, EncryptionRoot: "-"}
	properties := []zfs.Property{{Dataset: source.Name, Name: policy.Namespace + "enabled", Value: "on", Source: zfs.SourceLocal}, {Dataset: source.Name, Name: policy.Namespace + "local", Value: "backup/data", Source: zfs.SourceLocal}, {Dataset: source.Name, Name: lifecycle.LineageProperty, Value: fixtureLineage, Source: zfs.SourceLocal}, {Dataset: source.Name, Name: lifecycle.OwnerProperty, Value: fixtureInstallation, Source: zfs.SourceLocal}}
	view := View{Inventory: []zfs.Dataset{{Name: "tank", Type: zfs.Filesystem}, source, {Name: "backup", Type: zfs.Filesystem}}, Source: zfs.State{Objects: []zfs.Object{{Name: source.Name, Type: "filesystem", GUID: 1, CreateTXG: 1}}, Properties: properties}, DestinationIdentity: zfs.DatasetIdentity{Name: "backup", Type: zfs.Filesystem, GUID: 10, Pool: "backup", PoolGUID: 11}}
	for _, txg := range []uint64{2, 4} {
		metadata, err := lifecycle.NewMetadata(fixtureLineage, time.Date(2026, 9, 6, 12, int(txg), 0, 0, time.UTC))
		if err != nil {
			t.Fatal(err)
		}
		name := source.Name + "@" + metadata.Name()
		view.Source.Objects = append(view.Source.Objects, zfs.Object{Name: name, Type: "snapshot", GUID: 100 + txg, CreateTXG: txg})
		for key, value := range metadata.Properties() {
			view.Source.Properties = append(view.Source.Properties, zfs.Property{Dataset: name, Name: key, Value: value, Source: zfs.SourceLocal})
		}
	}
	view.Source.Objects = append(view.Source.Objects, zfs.Object{Name: source.Name + "@foreign", Type: "snapshot", GUID: 103, CreateTXG: 3})
	request := Request{Source: source.Name, DestinationRoot: "backup/data", Policy: policy.Resolve(source, nil, properties, nil)}
	return request, view
}

func addDestinationBase(view *View) {
	base := view.Source.Objects[1]
	base.Name = "backup/data" + strings.TrimPrefix(base.Name, "tank/data")
	base.CreateTXG = 101
	view.DestinationExists = true
	view.Inventory = append(view.Inventory, zfs.Dataset{Name: "backup/data", Type: zfs.Filesystem, EncryptionRoot: "-"})
	view.Destination = zfs.State{Objects: []zfs.Object{{Name: "backup/data", Type: "filesystem", GUID: 20, CreateTXG: 100}, base}}
	view.DestinationIdentity = zfs.DatasetIdentity{Name: "backup/data", Type: zfs.Filesystem, GUID: 20, Pool: "backup", PoolGUID: 11}
}

func TestBuildFullIncrementalAndNoop(t *testing.T) {
	t.Parallel()
	request, view := testFixture(t)
	plan, err := Build(request, view, fixtureInstallation)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Mode != "full" || len(plan.Expected) != 1 || plan.Snapshot != view.Source.Objects[2].Name || plan.Receive.Set["canmount"] != "noauto" {
		t.Fatalf("full plan=%v", plan)
	}
	addDestinationBase(&view)
	plan, err = Build(request, view, fixtureInstallation)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Mode != "incremental-all" || len(plan.Expected) != 2 || !plan.Send.Intermediates || !slices.Contains(plan.Warnings, "stream includes foreign snapshot: tank/data@foreign") {
		t.Fatalf("-I plan=%v", plan)
	}
	request.Policy.Incremental = "latest"
	plan, err = Build(request, view, fixtureInstallation)
	if err != nil || plan.Mode != "incremental-latest" || len(plan.Expected) != 1 {
		t.Fatalf("-i plan=%v err=%v", plan, err)
	}
	endpoint := view.Source.Objects[2]
	endpoint.Name = "backup/data" + strings.TrimPrefix(endpoint.Name, "tank/data")
	endpoint.CreateTXG = 102
	view.Destination.Objects = append(view.Destination.Objects, endpoint)
	plan, err = Build(request, view, fixtureInstallation)
	if err != nil || plan.Mode != "up-to-date" {
		t.Fatalf("noop=%v err=%v", plan, err)
	}
}

func TestBuildRemoteUsesIndependentDestinationInventory(t *testing.T) {
	t.Parallel()
	request, view := testFixture(t)
	request.Transport = "ssh"
	request.RemoteName = "home"
	request.CanonicalTarget = "ssh://replicator@backup.example.net:22/tank/data"
	request.DestinationRoot = "tank/data"
	request.Policy.Remote = []string{"home"}
	view.DestinationInventory = []zfs.Dataset{{Name: "tank", Type: zfs.Filesystem, EncryptionRoot: "-"}}
	view.DestinationIdentity = zfs.DatasetIdentity{Name: "tank", Type: zfs.Filesystem, GUID: 50, Pool: "tank", PoolGUID: 51}

	plan, err := Build(request, view, fixtureInstallation)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Mode != "full" || plan.Destination != "tank/data" || plan.TargetBinding.Transport != "ssh" || plan.TargetBinding.CanonicalTarget != request.CanonicalTarget {
		t.Fatalf("remote plan=%+v", plan)
	}
	if plan.TargetBinding.PoolGUID != 51 || plan.TargetBinding.Anchor != "tank" {
		t.Fatalf("remote destination identity was not used: %+v", plan.TargetBinding)
	}
	view.Source.Properties = append(view.Source.Properties, zfs.Property{Dataset: request.Source, Name: targetSuspendedProperty(request.CanonicalTarget), Value: "unverified", Source: zfs.SourceLocal})
	if _, err := Build(request, view, fixtureInstallation); err == nil || !strings.Contains(err.Error(), "suspended") {
		t.Fatalf("suspended remote was not rejected: %v", err)
	}
}

func TestBuildRefusesUnsafeDestinations(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"overlap", "unconfigured", "existing-empty", "foreign-latest", "resume-child", "name-collision", "bad-lineage", "foreign-owner", "default-activation"} {
		t.Run(kind, func(t *testing.T) {
			request, view := testFixture(t)
			addDestinationBase(&view)
			switch kind {
			case "overlap":
				request.DestinationRoot = "tank/data/backup"
				request.Policy.Local = []string{request.DestinationRoot}
			case "unconfigured":
				request.DestinationRoot = "backup/other"
			case "existing-empty":
				view.Destination.Objects = view.Destination.Objects[:1]
			case "foreign-latest":
				view.Destination.Objects = append(view.Destination.Objects, zfs.Object{Name: "backup/data@other", Type: "snapshot", GUID: 999, CreateTXG: 103})
			case "resume-child":
				view.Destination.ResumeTokens = map[string]string{"backup/data/child": "token"}
			case "name-collision":
				o := view.Source.Objects[2]
				o.Name = "backup/data" + strings.TrimPrefix(o.Name, "tank/data")
				o.GUID++
				view.Destination.Objects = append(view.Destination.Objects, o)
			case "bad-lineage":
				view.Destination.Properties = []zfs.Property{{Dataset: "backup/data", Name: lifecycle.LineageProperty, Value: "22222222-2222-4222-8222-222222222222", Source: zfs.SourceLocal}}
			case "foreign-owner":
				for i := range view.Source.Properties {
					if view.Source.Properties[i].Name == lifecycle.OwnerProperty {
						view.Source.Properties[i].Value = fixtureLineage
					}
				}
			case "default-activation":
				value := request.Policy.Values[policy.Namespace+"enabled"]
				value.Dataset = ""
				request.Policy.Values[policy.Namespace+"enabled"] = value
			}
			if _, err := Build(request, view, fixtureInstallation); err == nil {
				t.Fatalf("accepted %s", kind)
			}
		})
	}
}

func TestTargetBindingDetectsReplacementAndMappingDrift(t *testing.T) {
	t.Parallel()
	request, view := testFixture(t)
	initial, err := Build(request, view, fixtureInstallation)
	if err != nil || !initial.BindingNew || initial.TargetBinding.Anchor != "backup" || initial.TargetBinding.RelativePath != "data" {
		t.Fatalf("initial binding=%+v err=%v", initial, err)
	}
	encoded, err := encodeBinding(initial.TargetBinding)
	if err != nil {
		t.Fatal(err)
	}
	property := targetBindingProperty(initial.TargetBinding.CanonicalTarget)
	view.Source.Properties = append(view.Source.Properties, zfs.Property{Dataset: request.Source, Name: property, Value: encoded, Source: zfs.SourceLocal})
	if _, err := Build(request, view, fixtureInstallation); err != nil {
		t.Fatalf("stable bootstrap binding rejected: %v", err)
	}

	// A newly appearing same-named destination cannot inherit authority from the
	// ancestor-only bootstrap binding. Successful Apply promotes the binding only
	// after received GUID verification.
	addDestinationBase(&view)
	if _, err := Build(request, view, fixtureInstallation); err == nil {
		t.Fatal("ancestor binding authorized an unverified new destination")
	}
	wanted, err := bindingFor(request, "backup/data", view.DestinationIdentity)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ = encodeBinding(wanted)
	for i := range view.Source.Properties {
		if view.Source.Properties[i].Name == property {
			view.Source.Properties[i].Value = encoded
		}
	}
	if _, err := Build(request, view, fixtureInstallation); err != nil {
		t.Fatalf("verified destination binding rejected: %v", err)
	}
	view.DestinationIdentity.GUID++
	if _, err := Build(request, view, fixtureInstallation); err == nil {
		t.Fatal("same-named replacement accepted")
	}
}

func TestVerifyTargetBindingRejectsSameNamedReplacement(t *testing.T) {
	t.Parallel()
	request, view := testFixture(t)
	plan, err := Build(request, view, fixtureInstallation)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := encodeBinding(plan.TargetBinding)
	if err != nil {
		t.Fatal(err)
	}
	view.Source.Properties = append(view.Source.Properties, zfs.Property{
		Dataset: request.Source,
		Name:    targetBindingProperty(plan.TargetBinding.CanonicalTarget),
		Value:   encoded,
		Source:  zfs.SourceLocal,
	})
	identity := view.DestinationIdentity
	backend := &localBackend{source: view.Source, identity: &identity}
	if _, err := VerifyTargetBinding(t.Context(), backend, view.Source, request.Source, plan.TargetBinding.CanonicalTarget, request.DestinationRoot); err != nil {
		t.Fatalf("stable target rejected: %v", err)
	}
	identity.GUID++
	if _, err := VerifyTargetBinding(t.Context(), backend, view.Source, request.Source, plan.TargetBinding.CanonicalTarget, request.DestinationRoot); err == nil {
		t.Fatal("same-named replacement accepted")
	}
}

func TestBookmarkBaseRequiresRecordedTargetProof(t *testing.T) {
	t.Parallel()
	request, view := testFixture(t)
	addDestinationBase(&view)
	base := view.Source.Objects[1]
	metadata, err := ownership(view.Source, base, fixtureLineage)
	if err != nil {
		t.Fatal(err)
	}
	ref := lifecycle.Reference{Target: "local:backup/data", GUID: base.GUID, Metadata: metadata}
	view.Source.Objects[1] = zfs.Object{Name: ref.BookmarkName(request.Source), Type: "bookmark", GUID: base.GUID, CreateTXG: base.CreateTXG}
	request.Policy.Incremental = "latest"
	if _, err := Build(request, view, fixtureInstallation); err == nil {
		t.Fatal("claimed name-only bookmark")
	}
	value, err := json.Marshal(ref)
	if err != nil {
		t.Fatal(err)
	}
	view.Source.Properties = append(view.Source.Properties, zfs.Property{Dataset: request.Source, Name: lifecycle.ReferencePrefix + lifecycle.TargetID(ref.Target) + ":" + metadata.Snapshot, Value: string(value), Source: zfs.SourceLocal})
	plan, err := Build(request, view, fixtureInstallation)
	if err != nil || plan.Base != ref.BookmarkName(request.Source) {
		t.Fatalf("bookmark=%v err=%v", plan, err)
	}
	request.Policy.Incremental = "all"
	if _, err := Build(request, view, fixtureInstallation); err == nil {
		t.Fatal("used bookmark for -I")
	}
}

func TestBuildRecursiveEndpointsAndNamespaceExclusions(t *testing.T) {
	t.Parallel()
	request, view := testFixture(t)
	request.Policy.Send.Replicate = true
	request.Policy.Send.Props = true
	rootEnd := view.Source.Objects[2]
	childEnd := rootEnd
	childEnd.Name = "tank/data/child" + strings.TrimPrefix(rootEnd.Name, "tank/data")
	childEnd.GUID = 200
	view.Source.Objects = append(view.Source.Objects, zfs.Object{Name: "tank/data/child", Type: "filesystem", GUID: 2, CreateTXG: 1}, childEnd)
	view.Inventory = append(view.Inventory, zfs.Dataset{Name: "tank/data/child", Type: zfs.Filesystem, EncryptionRoot: "-"})
	for _, p := range slices.Clone(view.Source.Properties) {
		if p.Dataset == rootEnd.Name {
			p.Dataset = childEnd.Name
			view.Source.Properties = append(view.Source.Properties, p)
		}
	}
	view.Source.Properties = append(view.Source.Properties, zfs.Property{Dataset: "tank", Name: policy.Namespace + "ignore_prop:quota", Value: "on", Source: zfs.SourceLocal})
	plan, err := Build(request, view, fixtureInstallation)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Endpoints) != 2 || len(plan.Expected) != 4 || !slices.Contains(plan.Receive.Exclude, policy.Namespace+"ignore_prop:quota") {
		t.Fatalf("recursive plan=%v", plan)
	}
}
