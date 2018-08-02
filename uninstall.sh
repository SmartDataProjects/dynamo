#!/bin/bash

echo "Uninstalling dynamo."

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

INSTALL_PATH=$($READCONF paths.dynamo_base)
CONFIG_PATH=$($READCONF paths.config_path)
ARCHIVE_PATH=$($READCONF paths.archive_path)
SPOOL_PATH=$($READCONF paths.spool_path)
LOG_PATH=$($READCONF paths.log_path)
CLIENT_PATH=$($READCONF paths.client_path)
WEBSERVER=$($READCONF web.enabled)
FILEOP=$($READCONF file_operations.enabled)

### Delete files ###

if [[ $(uname -r) =~ el7 ]]
then
  systemctl stop dynamod
  systemctl disable dynamod

  rm /usr/lib/systemd/system/dynamod.service
  rm /etc/sysconfig/dynamod
else
  service dynamod stop
  chkconfig dynamod off

  rm /etc/init.d/dynamod
fi

FILEOP_BACKEND=$($READCONF file_operations.backend)

if [ "$FILEOP" = "true" ] && [ "$FILEOP_BACKEND" = "standalone" ]
then
  if [[ $(uname -r) =~ el7 ]]
  then
    systemctl stop dynamo-fileopd
    systemctl disable dynamo-fileopd
  
    rm /usr/lib/systemd/system/dynamo-fileopd.service
    rm /etc/sysconfig/dynamo-fileopd
  else
    service dynamo-fileopd stop
    chkconfig dynamo-fileopd off
  
    rm /etc/init.d/dynamo-fileopd
  fi
fi

rm -rf $INSTAL_PATH
rm -rf $SPOOL_PATH

for FILE in dynamo dynamo-inject dynamo-request
do
  rm $CLIENT_PATH/dynamo
done

for FILE in dynamod dynamo-exec-auth dynamo-fileopd dynamo-user-auth
do
  rm $SYSBIN_PATH/$FILE
done

for PYPATH in $(python -c 'import sys; print " ".join(sys.path)')
do
  if [[ $PYPATH =~ ^/usr/lib/python.*/site-packages$ ]]
  then
    rm -rf $PYPATH/dynamo
    break
  fi
done

# NRPE PLUGINS
if [ -d /usr/lib64/nagios/plugins ]
then
  rm /usr/lib64/nagios/plugins/check_dynamo.sh
fi

echo "The following elements are not deleted:"
echo " . DB tables"
echo " . $CONFIG_PATH"
echo " . $LOG_PATH"
echo " . $ARCHIVE_PATH"
echo " . crontab (if any entries were made)"
