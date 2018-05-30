#!/bin/bash

###########################################################################################
## install.sh
##
## Sets up HTML templates and contents.
###########################################################################################

THISDIR=$(cd $(dirname $0); pwd)

source $THISDIR/../utilities/shellutils.sh

echo '########################################'
echo '######  WEB SERVER CONFIGURATIONS ######'
echo '########################################'

require rpm -q lighttpd
require rpm -q lighttpd-fastcgi
if [[ $(getsebool httpd_setrlimit) =~ off ]]
then
  setsebool httpd_setrlimit 1
fi

if [[ $(uname -r) =~ el7 ]]
then
  LIGHTTPDCONF=/etc/sysconfig/lighttpd
  if ! [ -e $LIGHTTPDCONF ] || ! grep -q OPENSSL_ALLOW_PROXY_CERTS $LIGHTTPDCONF
  then
    echo "OPENSSL_ALLOW_PROXY_CERTS=1" >> $LIGHTTPDCONF
  fi
else
  LIGHTTPDINIT=/etc/init.d/lighttpd
  if ! grep -q OPENSSL_ALLOW_PROXY_CERTS $LIGHTTPDINIT
  then
    sed -i '/lockfile=/a OPENSSL_ALLOW_PROXY_CERTS=1' $LIGHTTPDINIT
  fi
fi

echo '#############################'
echo '######  HTML TEMPLATES ######'
echo '#############################'
echo

READCONF="$THISDIR/../utilities/readconf -I $THISDIR/../dynamo.cfg"

CONTENTS_PATH=$($READCONF web.contents_path)
mkdir -p $CONTENTS_PATH

cp -r $THISDIR/html $CONTENTS_PATH/html
