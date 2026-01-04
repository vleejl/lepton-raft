#!/bin/bash

set -ex

apt-get update
apt install -y build-essential
apt-get install -y gdb
apt-get install -y vim
apt-get install -y cmake
apt-get install -y git
apt-get install -y curl
apt-get install -y wget
apt-get install -y python3-pip
apt-get install -y clang clangd
apt-get install -y clang-format
apt-get install -y unzip
apt-get install -y liburing-dev

# Install xmakes
curl -fsSL https://xmake.io/shget.text | bash