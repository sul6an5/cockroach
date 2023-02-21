#!/bin/sh

clear

# update package list.
sudo apt-get update
# Install build-essential. This will install gcc
sudo apt install build-essential
# check g++ version
sudo apt-get install g++
g++ --version
# Installing the ncurses library in Debian/Ubuntu Linux
sudo apt-get install libncurses5-dev libncursesw5-dev
# Install "libresolv-wrapper" Package on Ubuntu

sudo apt-get update -y
sudo apt-get install -y libresolv-wrapper
# latest stable version of git
sudo apt-get install git
# install bash
sudo apt-get install bash
# install cmake
wget 
https://github.com/Kitware/CMake/releases/download/v3.20.0/cmake-3.20.0.tar.gz
tar -zvxf cmake-3.20.0.tar.gz
cd cmake-3.20.0
./bootstrap
make -j8
cd ..
# install autoconf
sudo apt-get install autoconf

sudo apt-get install bison flex
sudo apt-get install -y bison
sudo apt-get install patch

# bazelisk for bazel
wget wget https://github.com/bazelbuild/bazelisk/releases/download/v1.8.1/bazelisk-linux-amd64
chmod +x bazelisk-linux-amd64
sudo rm -rf /usr/local/bazel
sudo mv bazelisk-linux-amd64 /usr/local/bin/bazel
which bazel

wget https://dl.google.com/go/go1.17.7.linux-amd64.tar.gz 
sudo tar -xvf go1.17.7.linux-amd64.tar.gz
sudo mv go /usr/local
# change the golang path
echo export GOROOT=/usr/local/go
echo export GOPATH=$HOME
echo export PATH=$GOPATH/bin:$GOROOT/bin:$PATH
go version

# install CRDB source code  
git clone https://github.com/salemmohammed/cockroach.git
cd cockroach
# build the source code from source
./dev build
./dev doctor
