#!/usr/bin/env bash
set -euo pipefail

version=${1:-}
if [[ $# -gt 2 || ! $version =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z]+([.-][0-9A-Za-z]+)*)?$ ]]; then
    printf 'Usage: %s vMAJOR.MINOR.PATCH [output-dir]\n' "$0" >&2
    exit 2
fi

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
output_dir=${2:-"$repo_root/dist"}
if [[ $output_dir != /* ]]; then
    output_dir="$PWD/$output_dir"
fi

stage=$(mktemp -d "${TMPDIR:-/tmp}/idlemesh-release.XXXXXXXX")
trap 'rm -rf "$stage"' EXIT
package_dir="$stage/package"
artifacts="$stage/artifacts"
licenses="$package_dir/third-party"
mkdir -p "$package_dir/docs" "$licenses" "$artifacts"

cd "$repo_root"
cp README.md LICENSE "$package_dir/"
cp docs/architecture.md "$package_dir/docs/"

go_root=$(go env GOROOT)
go_license="$go_root/LICENSE"
if [[ ! -f $go_license ]]; then
    # Homebrew installs LICENSE alongside libexec (GOROOT).
    go_license="$go_root/../LICENSE"
fi
cp "$go_license" "$licenses/go-LICENSE"
go mod download go.etcd.io/bbolt golang.org/x/sys github.com/ebitengine/purego
bbolt_dir=$(go list -m -f '{{.Dir}}' go.etcd.io/bbolt)
x_sys_dir=$(go list -m -f '{{.Dir}}' golang.org/x/sys)
purego_dir=$(go list -m -f '{{.Dir}}' github.com/ebitengine/purego)
cp "$bbolt_dir/LICENSE" "$licenses/bbolt-LICENSE"
cp "$x_sys_dir/LICENSE" "$licenses/x-sys-LICENSE"
cp "$purego_dir/LICENSE" "$licenses/purego-LICENSE"

for os in darwin linux windows; do
    binary=idlemesh
    if [[ $os == windows ]]; then
        binary=idlemesh.exe
    fi
    for arch in amd64 arm64; do
        archive="idlemesh-$version-$os-$arch.zip"
        CGO_ENABLED=0 GOOS="$os" GOARCH="$arch" GOAMD64=v1 GOARM64=v8.0 \
            go build -mod=readonly -trimpath -ldflags "-X main.version=$version" \
            -o "$package_dir/$binary" ./cmd/idlemesh
        (cd "$package_dir" && zip -q "$artifacts/$archive" "$binary" README.md LICENSE docs/architecture.md third-party/*)
    done
done

(
    cd "$artifacts"
    if command -v sha256sum >/dev/null; then
        sha256sum -- *.zip
    else
        shasum -a 256 *.zip
    fi
) > "$artifacts/SHA256SUMS"

mkdir -p "$output_dir"
cp "$artifacts"/*.zip "$output_dir/"
cp "$artifacts/SHA256SUMS" "$output_dir/"
printf 'Packaged %s in %s\n' "$version" "$output_dir"
