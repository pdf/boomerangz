#!/usr/bin/env bash

set -euo pipefail

readonly source_repository=${1:?usage: publish-docs.sh SOURCE_REPOSITORY PUBLISHED_DIRECTORY}
readonly published_directory=${2:?usage: publish-docs.sh SOURCE_REPOSITORY PUBLISHED_DIRECTORY}
readonly docs_event=${DOCS_EVENT:?DOCS_EVENT must be branch or tag}
readonly docs_ref_name=${DOCS_REF_NAME:?DOCS_REF_NAME is required}
readonly runner_temp=${RUNNER_TEMP:?RUNNER_TEMP is required}

[[ $docs_event == branch || $docs_event == tag ]] || {
	printf 'unsupported documentation event: %s\n' "$docs_event" >&2
	exit 1
}
if [[ $docs_event == branch && $docs_ref_name != main ]]; then
	printf 'development documentation must be published from main\n' >&2
	exit 1
fi
if [[ $docs_event == tag && ! $docs_ref_name =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
	printf 'invalid documentation release tag: %s\n' "$docs_ref_name" >&2
	exit 1
fi

versions=$(git -C "$source_repository" tag --list --sort=-version:refname |
	jq -R -s -c 'split("\n") | map(select(test("^v[0-9]+\\.[0-9]+\\.[0-9]+$")))')
readonly versions
latest_version=$(jq -r '.[0] // empty' <<<"$versions")
readonly latest_version

build_docs() {
	local ref=$1 version=$2 base=$3 output=$4 canonical_base=${5:-}
	local worktree safe_ref
	safe_ref=${ref//[^A-Za-z0-9._-]/-}
	worktree=$runner_temp/boomerangz-docs-source-$safe_ref

	git -C "$source_repository" worktree add --detach "$worktree" "$ref"
	(
		cd "$worktree/docs"
		npm ci
		DOCS_VERSION=$version \
			DOCS_REF=$ref \
			DOCS_BASE=$base \
			DOCS_CANONICAL_BASE=${canonical_base:-$base} \
			npm run build -- --outDir "$output"
	)
	git -C "$source_repository" worktree remove --force "$worktree"
}

replace_root_docs() {
	local ref=$1 version=$2 canonical_base=$3 entry
	local output=$runner_temp/boomerangz-docs-root
	local manifest=$published_directory/.root-manifest
	local next_manifest=$runner_temp/boomerangz-docs-root-manifest

	build_docs "$ref" "$version" / "$output" "$canonical_base"
	if [[ -f $manifest ]]; then
		while IFS= read -r entry; do
			[[ $entry =~ ^[A-Za-z0-9._-]+$ ]] || {
				printf 'invalid root documentation entry: %s\n' "$entry" >&2
				exit 1
			}
			rm -rf -- "${published_directory:?}/$entry"
		done <"$manifest"
	fi
	find "$output" -mindepth 1 -maxdepth 1 -printf '%f\n' | sort >"$next_manifest"
	cp -a "$output/." "$published_directory/"
	cp "$next_manifest" "$manifest"
}

if [[ $docs_event == branch ]]; then
	development_output=$runner_temp/boomerangz-docs-development
	build_docs main Development /development/ "$development_output"
	rm -rf -- "${published_directory:?}/development"
	mv "$development_output" "$published_directory/development"
elif [[ ! -f $published_directory/$docs_ref_name/index.html ]]; then
	build_docs "$docs_ref_name" "$docs_ref_name" "/$docs_ref_name/" \
		"$published_directory/$docs_ref_name"
fi

if [[ -z $latest_version ]]; then
	if [[ $docs_event == branch ]]; then
		replace_root_docs main Development /
	fi
elif [[ ! -f $published_directory/index.html || $docs_ref_name == "$latest_version" ]]; then
	replace_root_docs "$latest_version" "$latest_version" "/$latest_version/"
fi

jq -n --argjson versions "$versions" --arg latest "$latest_version" \
	'{latest: $latest, versions: $versions}' >"$published_directory/versions.json"
