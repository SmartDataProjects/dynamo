#!/bin/bash

### EDIT THIS ###

CLIENT_PATH=/usr/bin

### Where we are installing from (i.e. this directory) ###

export SOURCE=$(cd $(dirname ${BASH_SOURCE[0]}); pwd)

### Install python libraries ###

echo "-> Installing.."

for PYPATH in $(python -c 'import sys; print " ".join(sys.path)')
do
  if [[ $PYPATH =~ ^/usr/lib/python.*/site-packages$ ]]
  then
    mkdir -p $PYPATH/dynamo
    mkdir -p $PYPATH/dynamo/client
    mkdir -p $PYPATH/dynamo/utils
    mkdir -p $PYPATH/dynamo/dataformat

    touch $PYPATH/dynamo/__init__.py

    cp -r $SOURCE/lib/client/* $PYPATH/dynamo/client
    cp -r $SOURCE/lib/utils/* $PYPATH/dynamo/utils
    cp -r $SOURCE/lib/dataformat/* $PYPATH/dynamo/dataformat
    python -m compileall $PYPATH/dynamo > /dev/null
  fi
done

### Install ###

mkdir -p $CLIENT_PATH
for FILE in dynamo dynamo-inject dynamo-request
do
  cp $SOURCE/bin/$FILE $CLIENT_PATH/$FILE
  chmod 755 $CLIENT_PATH/$FILE
done

echo " Done."
echo
