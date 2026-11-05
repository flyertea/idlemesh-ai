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
if [[ -f docs/ha-setup.md ]]; then
    cp docs/ha-setup.md "$package_dir/docs/"
fi

go_root=$(go env GOROOT)
go_license="$go_root/LICENSE"
if [[ ! -f $go_license ]]; then
    # Homebrew installs LICENSE alongside libexec (GOROOT).
    go_license="$go_root/../LICENSE"
fi
cp "$go_license" "$licenses/go-LICENSE"
if [[ -f "$go_root/PATENTS" ]]; then
    cp "$go_root/PATENTS" "$licenses/go-PATENTS"
fi

targets=(darwin-amd64 darwin-arm64 linux-amd64 linux-arm64 windows-amd64 windows-arm64)
build_info="$stage/build-info"
for target in "${targets[@]}"; do
    os=${target%-*}
    arch=${target#*-}
    binary=idlemesh
    if [[ $os == windows ]]; then
        binary=idlemesh.exe
    fi
    mkdir -p "$stage/$target"
    CGO_ENABLED=0 GOOS="$os" GOARCH="$arch" GOAMD64=v1 GOARM64=v8.0 \
        go build -mod=readonly -trimpath -ldflags "-X main.version=$version" \
        -o "$stage/$target/$binary" ./cmd/idlemesh
    go version -m "$stage/$target/$binary" >> "$build_info"
done

# Use the modules linked into the six binaries so target-specific libraries
# appear in the notices and test-only dependencies stay out.
if ! dependencies=$(awk '
    $1 == "dep" { print $2 "\t" $3 }
    $1 == "=>" { replaced = 1 }
    END { if (replaced) exit 1 }
' "$build_info" | LC_ALL=C sort -u); then
    printf 'Cannot publish binaries with replaced Go modules.\n' >&2
    exit 1
fi
printf 'Source code for the Go modules included in this release.\n\n' > "$licenses/SOURCES.txt"
while IFS=$'\t' read -r module dependency_version; do
    if [[ -z $module ]]; then
        continue
    fi
    module_dir=$(go list -mod=readonly -m -f '{{.Dir}}' "$module@$dependency_version")
    if [[ ! -d $module_dir ]]; then
        printf 'Source directory missing for %s %s\n' "$module" "$dependency_version" >&2
        exit 1
    fi
    destination="$licenses/$module"
    mkdir -p "$destination"
    copied=
    for notice in "$module_dir"/LICENSE* "$module_dir"/NOTICE* "$module_dir"/COPYING* "$module_dir"/PATENTS*; do
        if [[ -f $notice ]]; then
            cp "$notice" "$destination/"
            copied=yes
        fi
    done
    if [[ -z $copied ]]; then
        printf 'No license notice found for %s %s\n' "$module" "$dependency_version" >&2
        exit 1
    fi
    # The Go module proxy escapes capital letters as ! followed by lowercase.
    source_path=$(LC_ALL=C awk '{
        for (i = 1; i <= length($0); i++) {
            ch = substr($0, i, 1)
            printf "%s", ch ~ /[A-Z]/ ? "!" tolower(ch) : ch
        }
    }' <<< "$module/@v/$dependency_version.zip")
    printf '%s %s\n  https://proxy.golang.org/%s\n\n' "$module" "$dependency_version" "$source_path" >> "$licenses/SOURCES.txt"
done <<< "$dependencies"

for target in "${targets[@]}"; do
    binary=idlemesh
    if [[ $target == windows-* ]]; then
        binary=idlemesh.exe
    fi
    cp "$stage/$target/$binary" "$package_dir/"
    (cd "$package_dir" && zip -qr "$artifacts/idlemesh-$version-$target.zip" "$binary" README.md LICENSE docs third-party)
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
