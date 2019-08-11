if [ "$#" -ne 4 ]; then
  echo "Usage: $0 EXASOL_HOST_PORT EXASOL_BUCKET_GOPATH EXASOL_BUCKETFS_USERPASS GOLANG_LIB_DIR" >&2
  echo "        For example: $0 192.168.1.172:2580 \"default/go\" \"w:write\" https://github.com/pyurin/exasol-scripts-golang " >&2
  exit 1
fi

set -e
export EXASOL_HOST_PORT=$1
export EXASOL_BUCKET_GOPATH=$2
export EXASOL_BUCKETFS_USERPASS=$3
export GOLANG_LIB_PATH=$4

GOLANG_LIB_PATH=${GOLANG_LIB_PATH%.git}
GOLANG_LIB_PATH=${GOLANG_LIB_PATH%/}
IS_GITHUB_PATH=$(echo "$GOLANG_LIB_PATH" | egrep "^https:\/\/github.com/([^\\\/]+)/([^\\\/]+)$")
if [ -z "$IS_GITHUB_PATH" ]
then
    echo "Github lib path incorrect, please, use this path template: https://github.com/user/lib"
    exit 1
else
    echo "Github lib path: $GOLANG_LIB_PATH"
    GOLANG_LIB_FS_PATH=$(echo "$GOLANG_LIB_PATH" | sed 's|https://||g')
    echo "Lib fs path = $GOLANG_LIB_FS_PATH"
fi
GOLANG_LIB_NAME=${GOLANG_LIB_PATH%/} && export GOLANG_LIB_NAME="${GOLANG_LIB_NAME##*/}"
rm -rf ./tmp_libs/
mkdir ./tmp_libs/
git clone $GOLANG_LIB_PATH ./tmp_libs/
rm -rf ./tmp_libs/.git*
tar -zc -C ./tmp_libs/ . | curl -f -u $EXASOL_BUCKETFS_USERPASS -X PUT -T - http://$EXASOL_HOST_PORT/$EXASOL_BUCKET_GOPATH/src/$GOLANG_LIB_FS_PATH.tar.gz
rm -rf ./tmp_libs/
echo "Script successfully uploaded into http://$EXASOL_HOST_PORT/$EXASOL_BUCKET_GOPATH/src/$GOLANG_LIB_FS_PATH"
echo "Lib is expected to be available as $GOLANG_LIB_FS_PATH"
echo ""