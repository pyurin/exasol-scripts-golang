if [ "$#" -ne 4 ]; then
  echo "Usage: $0 EXASOL_HOST_PORT EXASOL_BUCKET_GOPATH EXASOL_BUCKETFS_USERPASS GOLANG_LIB_DIR" >&2
  echo "        For example: $0 192.168.1.172:2580 \"default/go\" \"w:write\" ~/go/src/numbers " >&2
  exit 1
fi

set -e
export EXASOL_HOST_PORT=$1
export EXASOL_BUCKET_GOPATH=$2
export EXASOL_BUCKETFS_USERPASS=$3
export GOLANG_LIB_PATH=$4

GOLANG_LIB_NAME=${GOLANG_LIB_PATH%/} && export GOLANG_LIB_NAME="${GOLANG_LIB_NAME##*/}"

tar -zc -C $GOLANG_LIB_PATH . | curl -f -u $EXASOL_BUCKETFS_USERPASS -X PUT -T - http://$EXASOL_HOST_PORT/$EXASOL_BUCKET_GOPATH/src/$GOLANG_LIB_NAME.tar.gz
echo "Script successfully uploaded into http://$EXASOL_HOST_PORT/$EXASOL_BUCKET_GOPATH/src/$GOLANG_LIB_NAME.tar.gz"
echo "Lib is expected to be available as $GOLANG_LIB_NAME"
echo ""