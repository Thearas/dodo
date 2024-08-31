#!/bin/bash

set -euo pipefail

os=$(uname -s)
if [ -n "${OS:-}" ]; then
    os="$OS"
fi

arch=$(uname -m)
if [ -n "${ARCH:-}" ]; then
    arch="$ARCH"
fi

os=$(echo "$os" | tr  "[:upper:]" "[:lower:]")
arch=$(echo "$arch" | tr  "[:upper:]" "[:lower:]")

if [[ "$arch" == "aarch64" ]]; then
    arch="arm64"
elif [[ "$arch" == "x86_64" ]]; then
    arch="amd64"
fi

binary_name="dodo-${os}-${arch}"

# get latest
latest_download_url=$(curl -s https://api.github.com/repos/Thearas/dodo/releases/latest | grep "browser_download_url.*${binary_name}" | cut -d : -f 2,3 | tr -d \")
if [[ -z "$latest_download_url" ]]; then
    echo "No release found for ${os}-${arch}"
    exit 1
fi

name=$(basename "$latest_download_url")

curl -SL $latest_download_url -o "/tmp/$name"
tar -xzf "/tmp/$name" -C /usr/local/bin/ && mv "/usr/local/bin/$binary_name" /usr/local/bin/dodo
rm -f "/tmp/$name"

echo "Installed successfully."

/usr/local/bin/dodo completion print-help
