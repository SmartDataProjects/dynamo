#!/bin/bash

echo "Uninstalling dynamo."

export SOURCE=$(cd $(dirname ${BASH_SOURCE[0]}); pwd)

source $SOURCE/config.sh

rm -rf $INSTALL_PATH

rm -rf $SCHEDULER_PATH

rm -rf $SPOOL_PATH

echo "The following elements are not deleted:"
echo " . DB tables"
echo " . $CONFIG_PATH"
echo " . $LOG_PATH"
echo " . $ARCHIVE_PATH"
echo " . crontab (if any entries were made)"

if [ $WEB_PATH ]
then
  export WEB_PATH
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

  # NRPE PLUGINS
  if [ -d /usr/lib64/nagios/plugins ]
  then
    rm /usr/lib64/nagios/plugins/check_dynamo.sh
  fi
fi
