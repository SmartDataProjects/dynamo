#!/bin/bash

DYNAMO_BASE=_DYNAMO_BASE_

source $DYNAMO_BASE/etc/profile.d/init.sh

cp $DYNAMO_BASE/sysv/detoxd /etc/init.d/
cp $DYNAMO_BASE/sysv/dealerd /etc/init.d/

mkdir -p $DYNAMO_LOGDIR
chmod 775 $DYNAMO_LOGDIR

crontab $DYNAMO_BASE/etc/crontab
