if [ "$#" -ne 3 ]; then
  echo "Usage: $0 EXASOL_HOST_PORT EXASOL_BUCKET_GOPATH EXASOL_BUCKETFS_USERPASS" >&2
  echo "        For example: $0 192.168.1.172:2580 \"/go/\" \"w:write\" " >&2
  exit 1
fi

export EXASOL_HOST_PORT=$1
export EXASOL_BUCKET_GOPATH=$2
export EXASOL_BUCKETFS_USERPASS=$3

set -eo pipefail

if [ ! -f ./src/golauncher.go ]
then
    echo "Must be run in project dir"
    exit 1;
fi

echo "Checking exasol image"
export EXASOL_OS_IMAGE=$(curl -f -s http://$EXASOL_HOST_PORT/default/@ | grep EXAClusterOS/ScriptLanguages.*.tar.gz | tail -n 1 | sed  's/\(^[0-9\\\t ]*\)//g')
if [ -z $EXASOL_OS_IMAGE ]
then
    echo "Could not get exasol image"
    exit 1;
fi
echo "Got os image: $EXASOL_OS_IMAGE"

echo "Importing docker image"
docker import http://$EXASOL_HOST_PORT/default/$EXASOL_OS_IMAGE  exasol-os-image;

echo "Building docker image"
docker build . -t exasol-scripts-golang-image;

echo "Cleaning up previously existing container (if any)"
docker stop exasol-scripts-golang-container && docker rm exasol-scripts-golang-container

echo "Staring container"
docker run --name exasol-scripts-golang-container exasol-scripts-golang-image

echo "Uploading container"
docker export exasol-scripts-golang-container | curl -f -u $EXASOL_BUCKETFS_USERPASS -X PUT -T - http://$EXASOL_HOST_PORT/$EXASOL_BUCKET_GOPATH/GolangImage.tar.gz

echo "Cleaning up container"
docker stop exasol-scripts-golang-container && docker rm exasol-scripts-golang-container

echo "Uploading scripts"
tar -zc ./src/ ./go.sh | curl -f -u $EXASOL_BUCKETFS_USERPASS -X PUT -T - http://$EXASOL_HOST_PORT/$EXASOL_BUCKET_GOPATH/go_entrypoint.tar.gz

echo "All done. Please, wait for a couple of minutes for exasol to extract archives."
echo "Then SCRIPT_LANGUAGES attribute should be appended with:"
echo " GO=localzmq+protobuf:///bfsdefault/$EXASOL_BUCKET_GOPATH/GolangImage?#buckets/bfsdefault/$EXASOL_BUCKET_GOPATH/go_entrypoint/go.sh"
echo ""
echo ""
echo "alter session set script_languages = 'PYTHON=builtin_python R=builtin_r JAVA=builtin_java GO=localzmq+protobuf:///bfsdefault/$EXASOL_BUCKET_GOPATH/GolangImage?#buckets/bfsdefault/$EXASOL_BUCKET_GOPATH/go_entrypoint/go.sh'"
echo ""
