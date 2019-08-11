if [ "$#" -ne 4 ]; then
  echo "Usage: $0 EXASOL_HOST_PORT EXASOL_BUCKET_GOPATH EXASOL_BUCKETFS_USERPASS GOLANG_LIB_DIR" >&2
  echo "        For example: $0 192.168.1.172:2580 \"default/go\" \"w:write\" ~/go/src/numbers " >&2
  exit 1
fi

set -eo pipefail
export EXASOL_HOST_PORT=$1
export EXASOL_BUCKET_GOPATH=$2
export EXASOL_BUCKETFS_USERPASS=$3
export GOLANG_LIB_PATH=$4

GOLANG_LIB_PATH=${GOLANG_LIB_PATH%.git}
GOLANG_LIB_PATH=${GOLANG_LIB_PATH%/}
if [[ $GOLANG_LIB_PATH =~ ^https:\/\/github.com/([^\\\/]+)/([^\\\/]+)$ ]]
then
    echo "Github lib path: $GOLANG_LIB_PATH"
    GOLANG_LIB_FS_PATH=${GOLANG_LIB_PATH//https:\/\/}
    echo "Lib fs path = $GOLANG_LIB_FS_PATH"
else
    echo "Github lib path incorrect, please, use this path template: https://github.com/user/lib"
    exit 1
fi
GOLANG_LIB_NAME=${GOLANG_LIB_PATH%/} && export GOLANG_LIB_NAME="${GOLANG_LIB_NAME##*/}"
rm -rf ./tmp_libs/
mkdir ./tmp_libs/
GITHUB_ARCHIVE_PATH=$GOLANG_LIB_PATH"/archive/master.zip"
curl -L $GITHUB_ARCHIVE_PATH -o ./tmp_libs/$GOLANG_LIB_NAME.zip
tar -xvf ./tmp_libs/$GOLANG_LIB_NAME.zip --directory ./tmp_libs/
rm ./tmp_libs/$GOLANG_LIB_NAME.zip
mv ./tmp_libs/$GOLANG_LIB_NAME-master ./tmp_libs/$GOLANG_LIB_NAME
tar -zc -C ./tmp_libs/$GOLANG_LIB_NAME . | curl -f -u $EXASOL_BUCKETFS_USERPASS -X PUT -T - http://$EXASOL_HOST_PORT/$EXASOL_BUCKET_GOPATH/src/$GOLANG_LIB_FS_PATH.tar.gz
rm -rf ./tmp_libs/
echo "Script successfully uploaded into http://$EXASOL_HOST_PORT/$EXASOL_BUCKET_GOPATH/src/$GOLANG_LIB_FS_PATH"
echo "Lib is expected to be available as $GOLANG_LIB_FS_PATH"
echo ""