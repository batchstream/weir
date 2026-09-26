#!/bin/sh
# Downloads only pinned public development tools into this repository's ignored directory.
set -eu
cd "$(dirname "$0")/.."
if [ "$(uname -s)" != Darwin ] || [ "$(uname -m)" != arm64 ]; then
 echo 'This qualification bootstrap is pinned to macOS arm64; see docs/milestone-1.md for the unqualified-platform boundary.' >&2
 exit 1
fi
mkdir -p .tools
if [ ! -f .tools/protoc.zip ]; then curl -fL --max-time 300 -o .tools/protoc.zip https://github.com/protocolbuffers/protobuf/releases/download/v33.4/protoc-33.4-osx-universal_binary.zip; fi
printf '%s  %s\n' 0745eb76adabd8ead6203cb094807e2e0a82f4222dcc7363986a611f02ca07ba .tools/protoc.zip | shasum -a 256 -c -
unzip -qo .tools/protoc.zip -d .tools/protoc
if [ ! -f .tools/mongodb.tgz ]; then curl -fL --max-time 300 -o .tools/mongodb.tgz https://fastdl.mongodb.org/osx/mongodb-macos-arm64-8.0.32.tgz; fi
printf '%s  %s\n' f81cb258434d548dca7244d599c82eb339043d8dedd0b1b807870c9d263117f2 .tools/mongodb.tgz | shasum -a 256 -c -
tar xzf .tools/mongodb.tgz -C .tools
