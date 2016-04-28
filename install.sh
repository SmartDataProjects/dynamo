#!/bin/bash

export DYNAMO_BASE=$(cd $(dirname ${BASH_SOURCE[0]}); pwd)
source $DYNAMO_BASE/etc/profile.d/init.sh

sed "s|_DYNAMO_BASE_|$DYNAMO_BASE|" $DYNAMO_BASE/sysv/detoxd > /etc/init.d/detoxd
sed "s|_DYNAMO_BASE_|$DYNAMO_BASE|" $DYNAMO_BASE/sysv/dealerd > /etc/init.d/dealerd

mkdir -p $DYNAMO_LOGDIR
chmod 775 $DYNAMO_LOGDIR

sed -i "s|_DYNAMO_BASE_|$DYNAMO_BASE|" $DYNAMO_BASE/etc/crontab | crontab
