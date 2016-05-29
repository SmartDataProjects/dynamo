#!/bin/bash

USER=$1

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
if [ -d /var/www/html ] && [ -d /var/www/cgi-bin ]
then
  mkdir -p /var/www/html/dynamo
  cp -r $DYNAMO_BASE/web/html/* /var/www/dynamo/
  mkdir -p $DYNAMO_BASE/web/cgi-bin/* /var/www/cgi-bin/dynamo
  ln -s /var/www/cgi-bin/dynamo/detox/main.php /var/www/html/dynamo/detox.php
  ln -s /var/www/cgi-bin/dynamo/inventory/main.php /var/www/html/dynamo/inventory.php
fi

sed -i "s|_DYNAMO_BASE_|$DYNAMO_BASE|" $DYNAMO_BASE/etc/crontab | crontab
