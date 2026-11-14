package discovery

import (
	"testing"

	"github.com/pdf/boomerangz/internal/policy"
)

func TestEntryStatus(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		entry Entry
		want  string
	}{
		{"invalid policy wins over everything", Entry{Inspected: true, CoveredBy: "tank", Member: true, Policy: policy.Effective{Enabled: true, Errors: []string{"bad"}}}, StatusInvalid},
		{"member of an actor root", Entry{CoveredBy: "tank", Member: true}, StatusMember},
		{"excluded under an actor root", Entry{CoveredBy: "tank"}, StatusExcluded},
		{"enabled root", Entry{Inspected: true, Policy: policy.Effective{Enabled: true}}, StatusActive},
		{"enabled but not inspected", Entry{Policy: policy.Effective{Enabled: true}}, StatusInactive},
		{"replica", Entry{Replica: Replica{Source: "tank/data"}}, StatusTarget},
		{"enabled replica is a root first", Entry{Inspected: true, Policy: policy.Effective{Enabled: true}, Replica: Replica{Source: "tank/data"}}, StatusActive},
		{"nothing", Entry{}, StatusInactive},
	} {
		if got := test.entry.Status(); got != test.want {
			t.Errorf("%s: Status() = %q, want %q", test.name, got, test.want)
		}
	}
}

func TestStatusFilter(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		statuses  []string
		kept      []string
		defaulted bool
	}{
		{statuses: nil, kept: []string{StatusActive, StatusMember, StatusExcluded, StatusTarget, StatusInvalid}, defaulted: true},
		{statuses: []string{"all"}, kept: Statuses},
		{statuses: []string{"all", StatusActive}, kept: Statuses},
		{statuses: []string{StatusInactive, StatusTarget}, kept: []string{StatusInactive, StatusTarget}},
	} {
		keep, defaulted := StatusFilter(test.statuses)
		if defaulted != test.defaulted {
			t.Errorf("StatusFilter(%q) defaulted = %v, want %v", test.statuses, defaulted, test.defaulted)
		}
		for _, status := range Statuses {
			want := false
			for _, kept := range test.kept {
				want = want || kept == status
			}
			if keep(status) != want {
				t.Errorf("StatusFilter(%q) keeps %q = %v, want %v", test.statuses, status, keep(status), want)
			}
		}
	}
}
