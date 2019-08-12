if [ "$#" -ne 3 ]; then
  echo "Usage: $0 EXASOL_HOST_PORT EXASOL_BUCKET_GOPATH EXASOL_BUCKETFS_USERPASS" >&2
  echo "        For example: $0 192.168.1.172:2580 \"default/go\" \"w:write\" " >&2
  exit 1
fi

export EXASOL_HOST_PORT=$1
export EXASOL_BUCKET_GOPATH=$2
export EXASOL_BUCKETFS_USERPASS=$3

set -eo pipefail

./install_os_image.sh $@
./install_scripts.sh $@


echo "All done. Please, wait for a couple of minutes for exasol to extract archives."
echo "Then SCRIPT_LANGUAGES attribute should be appended with:"
echo " GO=localzmq+protobuf:///bfsdefault/$EXASOL_BUCKET_GOPATH/GolangImage?#buckets/bfsdefault/$EXASOL_BUCKET_GOPATH/src/exago.sh"
echo ""
echo ""
echo "ALTER SESSION SET SCRIPT_LANGUAGES='PYTHON=builtin_python R=builtin_r JAVA=builtin_java GO=localzmq+protobuf:///bfsdefault/$EXASOL_BUCKET_GOPATH/GolangImage?#buckets/bfsdefault/$EXASOL_BUCKET_GOPATH/src/exago.sh'"
echo ""
