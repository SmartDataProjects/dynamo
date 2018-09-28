#!/bin/bash

### Where we are installing from (i.e. this directory) ###

export SOURCE=$(cd $(dirname ${BASH_SOURCE[0]}); pwd)

### Read the config ###

INSTALL_CONF=$1
[ -z "$INSTALL_CONF" ] && INSTALL_CONF=$SOURCE/dynamo.cfg

if ! [ -e $INSTALL_CONF ]
then
  echo
  echo "$INSTALL_CONF does not exist."
  exit 1
fi

source $SOURCE/utilities/shellutils.sh

READCONF="$SOURCE/utilities/readconf -I $INSTALL_CONF"

CLIENT_PATH=$($READCONF paths.client_path)

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

    cp -r $SOURCE/dynamo/client/* $PYPATH/dynamo/client
    cp -r $SOURCE/dynamo/utils/* $PYPATH/dynamo/utils
    cp -r $SOURCE/dynamo/dataformat/* $PYPATH/dynamo/dataformat
    python -m compileall $PYPATH/dynamo > /dev/null

    break
  fi
done

### Install ###

mkdir -p $CLIENT_PATH
for FILE in dynamo dynamo-inject dynamo-request
do
  sed "s|_PYTHON_|$(which python)|" $SOURCE/bin/$FILE > $CLIENT_PATH/$FILE
  chmod 755 $CLIENT_PATH/$FILE
done

echo " Done."
echo
