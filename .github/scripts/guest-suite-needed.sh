#!/usr/bin/env bash
# Reads the paths a change touches, one per line, and prints guests=true when
# the guest integration suite has to run for it, or guests=false when every
# path is documentation that no guest can observe: Markdown anywhere, and the
# documentation site under docs/. An empty list prints guests=true, so a
# change whose paths could not be listed runs everything.
#
# Host validation and the documentation build run for every change regardless:
# they are quick, and host validation reads documentation too
# (TestActionVersionsMatchUpgradeNotes reads docs/operations/upgrading.md).

set -euo pipefail

documentation_only=no
while IFS= read -r path; do
	[[ -n $path ]] || continue
	case $path in
	docs/* | *.md) documentation_only=yes ;;
	*)
		printf 'guests=true\n'
		exit 0
		;;
	esac
done
if [[ $documentation_only == yes ]]; then
	printf 'guests=false\n'
else
	printf 'guests=true\n'
fi
