// Package scope applies destination-root boundaries to remote ZFS objects.
package scope

import "strings"

// Dataset returns the dataset portion of a filesystem, volume, snapshot, or bookmark name.
func Dataset(object string) string {
	if index := strings.IndexAny(object, "@#"); index >= 0 {
		return object[:index]
	}
	return object
}

// Inside reports whether an object belongs to root or one of its descendants.
func Inside(root, object string) bool {
	dataset := Dataset(object)
	return dataset == root || strings.HasPrefix(dataset, root+"/")
}

// Related reports whether a dataset is root, its ancestor, or its descendant.
func Related(root, dataset string) bool {
	return Inside(root, dataset) || strings.HasPrefix(root, dataset+"/")
}
