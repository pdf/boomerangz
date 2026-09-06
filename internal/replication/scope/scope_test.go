package scope

import "testing"

func TestInside(t *testing.T) {
	t.Parallel()
	tests := map[string]bool{
		"tank/backups":                true,
		"tank/backups/child":          true,
		"tank/backups@snapshot":       true,
		"tank/backups/child@snapshot": true,
		"tank/backups/child#bookmark": true,
		"tank/backup":                 false,
		"tank/backups-other@snapshot": false,
		"tank/backups-other#bookmark": false,
	}
	for object, expected := range tests {
		if actual := Inside("tank/backups", object); actual != expected {
			t.Errorf("Inside(tank/backups, %q) = %t, want %t", object, actual, expected)
		}
	}
}

func TestRelated(t *testing.T) {
	t.Parallel()
	for dataset, expected := range map[string]bool{
		"tank":               true,
		"tank/backups":       true,
		"tank/backups/child": true,
		"tank/other":         false,
	} {
		if actual := Related("tank/backups", dataset); actual != expected {
			t.Errorf("Related(tank/backups, %q) = %t, want %t", dataset, actual, expected)
		}
	}
}
