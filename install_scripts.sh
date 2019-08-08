if [ "$#" -ne 3 ]; then
  echo "Usage: $0 EXASOL_HOST_PORT EXASOL_BUCKET_GOPATH EXASOL_BUCKETFS_USERPASS" >&2
  echo "        For example: $0 192.168.1.172:2580 \"default/go\" \"w:write\" " >&2
  exit 1
fi

export EXASOL_HOST_PORT=$1
export EXASOL_BUCKET_GOPATH=$2
export EXASOL_BUCKETFS_USERPASS=$3

set -eo pipefail

echo "Uploading scripts"
curl -f -u $EXASOL_BUCKETFS_USERPASS -X PUT -T ./src/exago.sh http://$EXASOL_HOST_PORT/$EXASOL_BUCKET_GOPATH/src/exago.sh
curl -f -u $EXASOL_BUCKETFS_USERPASS -X PUT -T ./src/exago.go http://$EXASOL_HOST_PORT/$EXASOL_BUCKET_GOPATH/src/exago.go
tar -zc -C ./src/exago/ . | curl -f -u $EXASOL_BUCKETFS_USERPASS -X PUT -T - http://$EXASOL_HOST_PORT/$EXASOL_BUCKET_GOPATH/src/exago.tar.gz

echo "Script upload done"