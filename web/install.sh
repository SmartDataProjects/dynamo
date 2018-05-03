#!/bin/bash

###########################################################################################
## install.sh
##
## Sets up web scripts for Apache. To be phased out by WSGI + Lighttpd.
###########################################################################################

THISDIR=$(cd $(dirname $0); pwd)

source $THISDIR/../utilities/shellutils.sh

echo '################################'
echo '######  WEB DEPENDENCIES  ######'
echo '################################'
echo

echo "-> Checking dependencies.."

require pgrep -f httpd
require which php
require [ -e /etc/httpd/conf.d/ssl.conf ]
require php -r 'mysqli_connect_errno();'

TEST=$(sed -n 's/.*max_execution_time[^0-9]*\([0-9]*\)/\1/p' /etc/php.ini)
if ! [ $TEST ] || [ $TEST -lt 600 ]
then
  echo "!!! PHP max_execution_time is less than the recommended value of 10 minutes."
  echo
fi

# Also should check memory_limit

echo '#############################################'
echo '######  PHP SCRIPTS AND HTML TEMPLATES ######'
echo '#############################################'
echo

TARGET=/var/www

HTMLTARGET=$TARGET/html/dynamo
BINTARGET=$TARGET/cgi-bin
HTMLSOURCE=$THISDIR/html/dynamo
BINSOURCE=$THISDIR/cgi-bin

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

mv $HTMLTARGET/dynamo/dealermon /tmp/dealermon.$$ 2>/dev/null

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
[ -L $HTMLTARGET/registry/applock ] || ln -sf $BINTARGET/registry/activitylock.php $HTMLTARGET/registry/applock
[ -L $HTMLTARGET/registry/application ] || ln -sf $BINTARGET/registry/interface.php $HTMLTARGET/registry/application
[ -L $HTMLTARGET/registry/invalidation ] || ln -sf $BINTARGET/registry/invalidation.php $HTMLTARGET/registry/invalidation
[ -L $HTMLTARGET/registry/request ] || ln -sf $BINTARGET/registry/requests.php $HTMLTARGET/registry/request

mv /tmp/dealermon.$$/monitoring* $HTMLTARGET/dynamo/dealermon 2>/dev/null
[ $? -eq 0 ] && rm -rf /tmp/dealermon.$$
chmod 777 $HTMLTARGET/dynamo/dealermon

### Verify .htaccess override
MESSAGE=$(curl -s 'http://localhost/registry/application?greet')
if [ "$MESSAGE" != "Hello" ]
then
  echo "Web server installation failed. This is most likely due to the base http server configuration."
  echo "Please check if the following line is under the root directory configuration directive:"
  echo "   AllowOverride FileInfo Options AuthConfig"
  exit 1
fi

echo "Please confirm that the web server is capable of serving requests using X509 certificate proxies."
echo "In particular, for apache, the environment variable"
echo "export OPENSSL_ALLOW_PROXY_CERTS=1"
echo "must be set in the httpd execution environment."
echo "The line above should be placed in /etc/init.d/httpd (SL6) or /etc/sysconfig/httpd (CentOS 7, without export)."

echo '#######################################'
echo '######  NEW-STYLE HTML TEMPLATES ######'
echo '#######################################'
echo

READCONF="$THISDIR/../utilities/readconf -I $THISDIR/../dynamo.cfg"

CONTENTS_PATH=$($READCONF web.contents_path)

mkdir -p $CONTENTS_PATH/html
cp -r $HTMLSOURCE/*.html $CONTENTS_PATH/html

ln -s $HTMLTARGET/dynamo/dynamo/css $CONTENTS_PATH/css
ln -s $HTMLTARGET/dynamo/dynamo/js $CONTENTS_PATH/js

exit 0
