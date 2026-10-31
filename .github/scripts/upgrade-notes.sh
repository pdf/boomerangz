#!/usr/bin/env bash
# Prints the upgrade notes docs/operations/upgrading.md holds for one release
# tag, as a release header leading with what operators must do, or nothing
# when that release needs nothing.

set -euo pipefail

readonly tag=${1:?usage: upgrade-notes.sh TAG [NOTES]}
readonly notes=${2:-docs/operations/upgrading.md}

[[ $tag =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || {
	printf 'not a release tag: %s\n' "$tag" >&2
	exit 1
}

section=$(awk -v heading="## $tag" '
	$0 == heading { found = 1; next }
	found && /^## / { exit }
	found { print }
' "$notes")
[[ -n ${section//[[:space:]]/} ]] || exit 0
# The site's links are relative to its root; the release page is elsewhere.
section=${section//](\//](https:\/\/boomerangz.org\/}

# VitePress anchors a heading by its text with dots turned to dashes.
anchor=${tag//./-}
printf '## Before upgrading\n%s\n\nThese notes are also at https://boomerangz.org/operations/upgrading#%s.\n\n' \
	"$section" "$anchor"
