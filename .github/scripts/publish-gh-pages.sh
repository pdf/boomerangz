#!/usr/bin/env bash
# Publishes one change to the gh-pages branch, which serves boomerangz.org: the
# documentation workflow's tree for one ref, and the release workflow's apt
# repository. Those runs are not serialised against each other - a concurrency
# group holds one waiting run and cancels any older one, so sharing a group
# dropped a release's publication whenever another run queued behind it - so
# any of them may find gh-pages has moved by the time it pushes.
#
# Each attempt therefore starts from the current gh-pages, runs COMMAND to
# rebuild this run's part of the tree in WORKTREE, and commits PATHSPEC. A push
# that loses the race fetches again and redoes the whole attempt, so what lands
# is always this run's change on top of every change pushed before it, never a
# stale tree that would undo one.

set -euo pipefail

readonly usage='usage: publish-gh-pages.sh WORKTREE MESSAGE PATHSPEC COMMAND [ARG...]'
readonly worktree=${1:?$usage}
readonly message=${2:?$usage}
readonly pathspec=${3:?$usage}
shift 3
(($# > 0)) || {
	printf '%s\n' "$usage" >&2
	exit 1
}
readonly attempts=${GH_PAGES_ATTEMPTS:-5}

for ((attempt = 1; attempt <= attempts; attempt++)); do
	git fetch --force origin +gh-pages:refs/remotes/origin/gh-pages
	if [[ -e $worktree ]]; then
		git -C "$worktree" reset --hard origin/gh-pages
		git -C "$worktree" clean -ffdx
	else
		git worktree add -B gh-pages "$worktree" origin/gh-pages
	fi

	"$@"

	git -C "$worktree" add --all -- "$pathspec"
	if git -C "$worktree" diff --cached --quiet; then
		printf 'gh-pages already holds this change.\n'
		exit 0
	fi
	git -C "$worktree" -c user.name=github-actions \
		-c user.email=github-actions@github.com commit --quiet -m "$message"
	if git -C "$worktree" push origin HEAD:gh-pages; then
		exit 0
	fi
	printf 'gh-pages moved during attempt %d of %d; rebuilding on the new tip.\n' \
		"$attempt" "$attempts" >&2
	sleep $((attempt * 5))
done

printf 'could not publish to gh-pages after %d attempts\n' "$attempts" >&2
exit 1
