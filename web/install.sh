#!/bin/bash

echo
echo "Installing web scripts."
echo

require () {
  "$@" >/dev/null 2>&1 && return 0
  echo
  echo "[Fatal] Failed: $@"
  exit 1
}

### Source the configuration parameters

export SOURCE=$(cd $(dirname ${BASH_SOURCE[0]})/..; pwd)

if ! [ -e $SOURCE/config.sh ]
then
  echo
  echo "$SOURCE/config.sh does not exist."
  exit 1
fi

require source $SOURCE/config.sh

### Verify dependencies ###

require pgrep -f httpd
require which php
require [ -e /etc/httpd/conf.d/ssl.conf ]
require php -r 'mysqli_connect_errno();'

TEST=$(sed -n 's/.*max_execution_time[^0-9]*\([0-9]*\)/\1/p' /etc/php.ini)
if ! [ $TEST ] || [ $TEST -lt 600 ]
then
  echo "!!! PHP max_execution_time is less than the recommended value of 10 minutes."
fi

# Also should check memory_limit

### Copy files ###

TARGET=$WEB_PATH

HTMLTARGET=$TARGET/html/dynamo
BINTARGET=$TARGET/cgi-bin
HTMLSOURCE=$SOURCE/web/html/dynamo
BINSOURCE=$SOURCE/web/cgi-bin

if [ -e $BINTARGET/dynamo/common/db_conf.php ]
then
  # move the config out of the way
  mv $BINTARGET/dynamo/common/db_conf.php /tmp/db_conf.php.$$
else
  echo
  echo "$BINTARGET/dynamo/common/db_conf.php does not exist. "
  echo "Without the configuration file, most of the web applications will not function."
  echo "Template exists at $BINSOURCE/dynamo/common/db_conf.php.template."
  echo
fi

mkdir -p $HTMLTARGET
for SUBDIR in $(ls $HTMLSOURCE)
do
  rm -rf $HTMLTARGET/$SUBDIR
  cp -r $HTMLSOURCE/$SUBDIR $HTMLTARGET/
done

mkdir -p $BINTARGET
for SUBDIR in $(ls $BINSOURCE)
do
  rm -rf $BINTARGET/$SUBDIR
  cp -r $BINSOURCE/$SUBDIR $BINTARGET/
done

# remove the template and check if the configuration exists
rm $BINTARGET/dynamo/common/db_conf.php.template
[ -e /tmp/db_conf.php.$$ ] && mv /tmp/db_conf.php.$$ $BINTARGET/dynamo/common/db_conf.php

[ -L $HTMLTARGET/dynamo/detox.php ] || ln -sf $BINTARGET/dynamo/detox/main.php $HTMLTARGET/dynamo/detox.php
[ -L $HTMLTARGET/dynamo/detoxlocks.php ] || ln -sf $BINTARGET/dynamo/detox/locks.php $HTMLTARGET/dynamo/detoxlocks.php
[ -L $HTMLTARGET/dynamo/inventory.php ] || ln -sf $BINTARGET/dynamo/inventory/main.php $HTMLTARGET/dynamo/inventory.php
[ -L $HTMLTARGET/registry/detoxlock ] || ln -sf $BINTARGET/registry/detoxlock.php $HTMLTARGET/registry/detoxlock
[ -L $HTMLTARGET/registry/activitylock ] || ln -sf $BINTARGET/registry/activitylock.php $HTMLTARGET/registry/activitylock
[ -L $HTMLTARGET/registry/application ] || ln -sf $BINTARGET/registry/interface.php $HTMLTARGET/registry/application

chmod 777 $HTMLTARGET/dynamo/dealermon
