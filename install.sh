#!/bin/bash

USER=$1

if ! [ $USER ]
then
  "Using cmsprod as the user of the executables."
  USER=cmsprod
fi

export DYNAMO_BASE=$(cd $(dirname ${BASH_SOURCE[0]}); pwd)
source $DYNAMO_BASE/etc/profile.d/init.sh

# DAEMONS
sed "s|_DYNAMO_BASE_|$DYNAMO_BASE|" $DYNAMO_BASE/sysv/dynamo-detoxd > /etc/init.d/dynamo-detoxd
sed "s|_DYNAMO_BASE_|$DYNAMO_BASE|" $DYNAMO_BASE/sysv/dynamo-dealerd > /etc/init.d/dynamo-dealerd
chmod +x /etc/init.d/dynamo-detoxd
chmod +x /etc/init.d/dynamo-dealerd

# DIRECTORIES
mkdir -p $DYNAMO_LOGDIR
chmod 775 $DYNAMO_LOGDIR
chown root:$(id -gn $USER) $DYNAMO_LOGDIR

mkdir -p $DYNAMO_DATADIR
chmod 775 $DYNAMO_DATADIR
chown root:$(id -gn $USER) $DYNAMO_DATADIR

# WEB INTERFACE
$DYNAMO_BASE/web/install.sh

sed -i "s|_DYNAMO_BASE_|$DYNAMO_BASE|" $DYNAMO_BASE/etc/crontab | crontab
