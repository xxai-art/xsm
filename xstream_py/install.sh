#!/usr/bin/env bash

DIR=$(realpath $0) && DIR=${DIR%/*}
cd $DIR
set -ex

rm -rf target/wheels
./build.sh
cd target/wheels
pip install --force-reinstall *.whl
