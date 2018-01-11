#!/bin/bash

echo "Uninstalling dynamo."

export SOURCE=$(cd $(dirname ${BASH_SOURCE[0]}); pwd)

source $SOURCE/config.sh

rm -rf $INSTALLPATH

rm -rf $CONFIGPATH

rm -rf $LOGPATH

rm -rf $SCHEDULERPATH

#rm -rf $ARCHIVEPATH

rm -rf $SPOOLPATH

# Not dropping DB tables

if [ $WEBPATH ]
then
  export WEBPATH
  $SOURCE/web/uninstall.sh
fi

if [ $DAEMONS -eq 1 ]
then
  if [[ $(uname -r) =~ el7 ]]
  then
    systemctl stop dynamod
    systemctl disable dynamod
    systemctl stop dynamo-scheduled
    systemctl disable dynamo-scheduled

    rm /usr/lib/systemd/system/dynamod.service
    rm /usr/lib/systemd/system/dynamo-scheduled.service
    rm /etc/sysconfig/dynamod
  else
    service stop dynamod
    chkconfig dynamod off
    service stop dynamo-scheduled
    chkconfig dynamo-scheduled off

    rm /etc/init.d/dynamod
    rm /etc/init.d/dynamo-scheduled
  fi

  # Not cleaning crontab

  # NRPE PLUGINS
  if [ -d /usr/lib64/nagios/plugins ]
  then
    rm /usr/lib64/nagios/plugins/check_dynamo.sh
  fi
fi
