#!/bin/sh
set -e

BASEDIR=`realpath $(dirname "$0")`

export GOCACHE=/tmp/go_cache/
export GOPATH=/var/lib/go/
export PATH=$PATH:/usr/local/go/bin:/bin

#@mkdir('/tmp/golang_cache');
#(HOME=/tmp/golang_cache/ PATH="$PATH:$BASEDIR/../golang/bin" go run $BASEDIR/golauncher.go $1 $2 $3 $4)

#(HOME=/tmp/golang_cache/ PATH="$PATH:$BASEDIR/../golang/bin" go run $BASEDIR/golauncher.go $1 $2 $3 $4)

if [ ! -f /tmp/golang/ ]
then
	mkdir /tmp/golang/
fi
if [ ! -f $BASEDIR/golauncher ]
then
	echo "Building launcher"
	#(GOPATH=$GOPATH:$BASEDIR go build -x -i -o /tmp/golang/golauncher $BASEDIR/*.go)
	GOPATH=$BASEDIR/:$GOPATH go build -o /tmp/golang/golauncher $BASEDIR/src/*.go
	echo "Build done"
fi
/tmp/golang/golauncher $1 $BASEDIR/ /tmp/go_cache/