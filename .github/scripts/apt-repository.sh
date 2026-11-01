#!/usr/bin/env bash
# Builds the signed apt repository published at https://boomerangz.org/apt from
# one release's .deb packages, signed with KEY from the GnuPG home the caller
# set up for this run alone: in the release workflow, a scratch home holding
# only the APT_SIGNING_KEY secret, whose signing subkey is all that is present
# while the primary's secret stays offline; in release validation, a home
# holding a throwaway key. KEY may name the primary or the subkey.
#
# The repository has one suite, stable, holding that release alone: the
# package is static, so one build serves every supported Ubuntu release, and
# earlier versions stay on their GitHub releases. It is rebuilt whole on every
# run, so nothing in it depends on what an earlier run left behind. The
# release workflow publishes it; release validation installs from a copy of it
# signed with a throwaway key, so what users install through is what was
# tested.

set -euo pipefail

readonly usage='usage: apt-repository.sh REPOSITORY KEY PACKAGE.deb...'
readonly repository=${1:?$usage}
readonly key=${2:?$usage}
shift 2
readonly suite=stable
readonly component=main

fail() {
	printf 'apt-repository.sh: %s\n' "$*" >&2
	exit 1
}

(($# > 0)) || fail "$usage"
for tool in apt-ftparchive dpkg-deb gpg gpgv gzip; do
	command -v "$tool" >/dev/null || fail "missing tool: $tool"
done

# Resolve KEY to the one key in that home that can sign and has its secret,
# rather than leaving it to gpg: a primary present only as a stub cannot sign.
# Anything other than exactly one such key means the home is not what the
# caller meant to provide, so stop.
mapfile -t signers < <(gpg --batch --with-colons --list-secret-keys -- "$key" | awk -F: '
	$1 == "sec" || $1 == "ssb" { candidate = $12 ~ /s/ && $15 == "+"; next }
	$1 == "fpr" && candidate { print $10; candidate = 0 }
')
((${#signers[@]} == 1)) ||
	fail "$key must have exactly one signing key with its secret present, found ${#signers[@]}"
readonly signer=${signers[0]}

install -d -m 0755 "$repository"
rm -rf -- "${repository:?}/pool" "${repository:?}/dists"
readonly pool=pool/$component/b/boomerangz
install -d -m 0755 "$repository/$pool"

architectures=()
for package in "$@"; do
	[[ $(dpkg-deb --field "$package" Package) == boomerangz ]] ||
		fail "not a boomerangz package: $package"
	architecture=$(dpkg-deb --field "$package" Architecture)
	[[ $architecture =~ ^[a-z0-9]+$ ]] || fail "unexpected architecture in $package"
	for seen in ${architectures[@]+"${architectures[@]}"}; do
		[[ $seen != "$architecture" ]] || fail "more than one $architecture package"
	done
	architectures+=("$architecture")
	install -m 0644 -- "$package" "$repository/$pool/"
done
mapfile -t architectures < <(printf '%s\n' "${architectures[@]}" | sort)

cd "$repository"
readonly dist=dists/$suite
for architecture in "${architectures[@]}"; do
	index=$dist/$component/binary-$architecture
	install -d -m 0755 "$index"
	apt-ftparchive --arch "$architecture" packages "$pool" >"$index/Packages"
	gzip -9 --no-name --keep "$index/Packages"
done

# The Release file hashes everything beneath dists/stable, so it is written
# outside that tree and moved in, rather than hashing a half-written self.
release=$(mktemp)
trap 'rm -f -- "$release"' EXIT
apt-ftparchive \
	-o APT::FTPArchive::Release::Origin=boomerangz \
	-o APT::FTPArchive::Release::Label=boomerangz \
	-o APT::FTPArchive::Release::Suite="$suite" \
	-o APT::FTPArchive::Release::Codename="$suite" \
	-o APT::FTPArchive::Release::Architectures="${architectures[*]}" \
	-o APT::FTPArchive::Release::Components="$component" \
	-o APT::FTPArchive::Release::Description='boomerangz release packages' \
	release "$dist" >"$release"
install -m 0644 "$release" "$dist/Release"

gpg --batch --yes --local-user "$signer!" --clearsign --output "$dist/InRelease" "$dist/Release"
gpg --batch --yes --local-user "$signer!" --armor --detach-sign --output "$dist/Release.gpg" "$dist/Release"
gpg --batch --yes --export "$signer" >boomerangz-archive-keyring.gpg
gpg --batch --yes --armor --export "$signer" >boomerangz-archive-keyring.asc
chmod 0644 "$dist/InRelease" "$dist/Release.gpg" boomerangz-archive-keyring.gpg boomerangz-archive-keyring.asc

# Check the signatures the way apt will: against the published keyring alone,
# so a repository users would reject is never published.
gpgv --keyring "$PWD/boomerangz-archive-keyring.gpg" "$dist/InRelease"
gpgv --keyring "$PWD/boomerangz-archive-keyring.gpg" "$dist/Release.gpg" "$dist/Release"

