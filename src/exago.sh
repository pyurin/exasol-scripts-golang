#!/bin/sh
set -e

BASEDIR=`realpath $(dirname "$0")/..`

export GOCACHE=/tmp/go_cache/
export GOPATH=/var/lib/go/
export PATH=$PATH:/usr/lib/go/bin:/bin

# Well, building is useless until there's no cache or any persistent storage
#if [ ! -f $BASEDIR/exago ]
#then
#    export GOPATH=$BASEDIR/:$GOPATH
#	echo "Building launcher with GOPATH=$GOPATH"
#	#(GOPATH=$GOPATH:$BASEDIR go build -x -i -o /tmp/golang/exago $BASEDIR/*.go)
#	go build -o /tmp/golang/exago $BASEDIR/src/*.go
#	echo "Build done"
#fi
#/tmp/golang/exago $1 $BASEDIR/ /tmp/go_cache/
####

GOPATH=$BASEDIR/:$GOPATH go run $BASEDIR/src/exago.go $1 $BASEDIR/ /tmp/go_cache/