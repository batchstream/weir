#!/bin/sh
# Installs the mongod used by the integration tests into .tools/ and prints its
# path. The archive is checked against its pinned SHA-256 before extraction.
set -eu
cd "$(dirname "$0")/.."
version=8.0.32
case "$(uname -s)/$(uname -m)" in
 Linux/x86_64)
  url=https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-ubuntu2404-$version.tgz
  sha256=b411be17c31ef249767ed91974d876e007c91afd5f45e1534057d247eada9f0d
  ;;
 Darwin/arm64)
  url=https://fastdl.mongodb.org/osx/mongodb-macos-arm64-$version.tgz
  sha256=f81cb258434d548dca7244d599c82eb339043d8dedd0b1b807870c9d263117f2
  ;;
 *) echo "no pinned mongod $version for $(uname -s)/$(uname -m)" >&2; exit 1;;
esac
install="$PWD/.tools/mongodb-$version"
if [ ! -e "$install" ]; then
 mkdir -p .tools
 work=$(mktemp -d "$install.XXXXXX")
 trap 'rm -rf "$work"' EXIT
 curl -fsSL --max-time 600 -o "$work/mongodb.tgz" "$url"
 printf '%s  %s\n' "$sha256" "$work/mongodb.tgz" | shasum -a 256 -c - >&2
 mkdir "$work/install"
 tar -xzf "$work/mongodb.tgz" -C "$work/install" --strip-components=1
 mv "$work/install" "$install"
fi
if [ ! -x "$install/bin/mongod" ]; then
 echo "$install has no mongod; remove it and run $0 again" >&2
 exit 1
fi
printf '%s\n' "$install/bin/mongod"
