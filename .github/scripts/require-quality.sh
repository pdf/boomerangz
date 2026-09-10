#!/usr/bin/env bash

set -euo pipefail

readonly quality_sha=${QUALITY_SHA:?QUALITY_SHA must name the commit to verify}
readonly repository=${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}

[[ $quality_sha =~ ^[0-9a-f]{40}$ ]] || {
	printf 'invalid quality commit: %s\n' "$quality_sha" >&2
	exit 1
}

git fetch --no-tags origin main
git merge-base --is-ancestor "$quality_sha" origin/main || {
	printf 'commit %s is not reachable from main\n' "$quality_sha" >&2
	exit 1
}

for _ in {1..120}; do
	runs=$(gh api \
		-H 'Accept: application/vnd.github+json' \
		-H 'X-GitHub-Api-Version: 2022-11-28' \
		"repos/$repository/actions/workflows/quality.yml/runs?head_sha=$quality_sha&per_page=100")

	if jq -e '.workflow_runs[] | select(
		.event == "push" and
		.head_branch == "main" and
		.status == "completed" and
		.conclusion == "success"
	)' <<<"$runs" >/dev/null; then
		printf 'Quality passed for %s on main\n' "$quality_sha"
		exit 0
	fi

	if jq -e '.workflow_runs[] | select(
		.event == "push" and
		.head_branch == "main" and
		.status == "completed" and
		.conclusion != "success"
	)' <<<"$runs" >/dev/null; then
		printf 'Quality did not pass for %s on main\n' "$quality_sha" >&2
		exit 1
	fi

	printf 'waiting for Quality to finish for %s\n' "$quality_sha"
	sleep 30
done

printf 'timed out waiting for Quality on %s\n' "$quality_sha" >&2
exit 1
