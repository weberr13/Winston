#!/bin/bash
####DONT RUN ON YOUR DEV BOX UNLESS YOU ARE TESTING THIS SCRIPT
#setup paths if they aren't set
theBranch="$1"
START=`pwd`
APP=Discover
TARGET=$START/target
mkdir -p $TARGET;
export GO15VENDOREXPERIMENT=1
if [ -z "$GOPATH" ]; then export GOPATH=~/go; fi
if [ -z "$GOBIN" ]; then export GOBIN=$GOPATH/bin; fi
if [ -z "$GOROOT" ]; then export GOROOT=/usr/local/go; fi
BASEDIR=$GOPATH/src/github.schq.secious.com/UltraViolet/
echo $GOPATH;
echo $GOBIN;
echo $GOROOT;
mkdir -p  $BASEDIR
cd $BASEDIR
set -e
cd $APP;
go build -o $TARGET/$APP
go test -v --race --timeout=5m $(go list ./... | grep -v vendor)


echo $GOPATH;
echo $GOBIN;
echo $GOROOT;

cd $START
