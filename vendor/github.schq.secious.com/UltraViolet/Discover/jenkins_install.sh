#!/bin/bash
shopt -s extglob  # sets extended pattern matching options in the bash shell
START=`pwd`
APP=Discover
export INSTALLDIR=go/src/github.schq.secious.com/UltraViolet/$APP
export GOROOT=/usr/local/go
mkdir -p $INSTALLDIR
export GOPATH=$START/go
export GOBIN=$GOPATH/bin
export PATH=$GOROOT/bin:$GOBIN:$PATH
cp -r `ls | egrep -v '^go$'` $INSTALLDIR/

BASEDIR=$GOPATH/src/github.schq.secious.com/UltraViolet/
echo $GOPATH;
echo $GOBIN;
echo $GOROOT;
mkdir -p  $BASEDIR
set -e
cd $INSTALLDIR;
go test -v --race --timeout=5m $(go list ./... | grep -v /vendor/)
#sh packaging/package.sh "$version.$buildnumber"
