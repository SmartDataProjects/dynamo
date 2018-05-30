#!/bin/bash

### Where we are installing from (i.e. this directory) ###

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

USER=$($READCONF server.user)
INSTALL_PATH=$($READCONF paths.dynamo_base)
CONFIG_PATH=$($READCONF paths.config_path)
ARCHIVE_PATH=$($READCONF paths.archive_path)
SPOOL_PATH=$($READCONF paths.spool_path)
LOG_PATH=$($READCONF paths.log_path)
POLICY_PATH=$($READCONF paths.policy_path)
WEBSERVER=$($READCONF web.enabled)
APPSERVER=$($READCONF applications.enabled)
SERVER_DB=$($READCONF server.store)

### Stop the daemons first ###

if [[ $(uname -r) =~ el7 ]]
then
  systemctl stop dynamod 2>/dev/null
else
  service dynamod stop 2>/dev/null
fi

echo
echo "Installing dynamo from $SOURCE."
echo

### Verify required components ###

echo '############################'
echo '######  DEPENDENCIES  ######'
echo '############################'
echo

echo "-> Checking dependencies.."

WARNING=false

require rpm -q python
warnifnot rpm -q condor-python
warnifnot rpm -q rrdtool-python
require rpm -q sqlite
if [ "$APPSERVER" = "true" ]
then
  if [[ $(uname -r) =~ el7 ]]
  then
    require rpm -q openssl-libs
  else
    require rpm -q openssl
  fi
fi
if [ "$WEBSERVER" = "true" ]
then
  require rpm -q python-flup
fi

if $WARNING
then
  echo " Some components may not work."
  echo
else
  echo " OK"
  echo
fi

echo '#########################'
echo '######  LIBRARIES  ######'
echo '#########################'
echo

### (Clear &) Make the directories ###

if [ -d $INSTALL_PATH ]
then
  echo "Target directory $INSTALL_PATH exists. Overwrite [y/n]?"
  if confirmed
  then
    rm -rf $INSTALL_PATH
  else
    echo "Exiting."
    exit 1
  fi
fi
echo

require mkdir -p $INSTALL_PATH
require mkdir -p $INSTALL_PATH/python/site-packages/dynamo
require mkdir -p $INSTALL_PATH/bin
require mkdir -p $INSTALL_PATH/exec
require mkdir -p $INSTALL_PATH/utilities
require mkdir -p $INSTALL_PATH/sbin
require mkdir -p $INSTALL_PATH/etc/profile.d
chown -R $USER:$(id -gn $USER) $INSTALL_PATH

require mkdir -p $CONFIG_PATH

require mkdir -p $LOG_PATH
chown $USER:$(id -gn $USER) $LOG_PATH

require mkdir -p $ARCHIVE_PATH
chown $USER:$(id -gn $USER) $ARCHIVE_PATH

mkdir -p $SPOOL_PATH
chown $USER:$(id -gn $USER) $SPOOL_PATH
chmod 777 $SPOOL_PATH

### Install python libraries ###

echo "-> Installing.."

cp -r $SOURCE/lib/* $INSTALL_PATH/python/site-packages/dynamo/
python -m compileall $INSTALL_PATH/python/site-packages/dynamo > /dev/null

### Install the executables ###

cp $SOURCE/bin/dynamo /usr/local/bin/

cp $SOURCE/exec/* $INSTALL_PATH/exec/
chown $USER:$(id -gn $USER) $INSTALL_PATH/exec/*
chmod 755 $INSTALL_PATH/exec/*

cp $SOURCE/utilities/* $INSTALL_PATH/utilities/
chown $USER:$(id -gn $USER) $INSTALL_PATH/utilities/*
chmod 755 $INSTALL_PATH/utilities/*

cp $SOURCE/sbin/* $INSTALL_PATH/sbin/
chown root:$(id -gn $USER) $INSTALL_PATH/sbin/*
chmod 754 $INSTALL_PATH/sbin/*

cp $SOURCE/etc/default_partitions.txt $INSTALL_PATH/etc/
chown root:$(id -gn $USER) $INSTALL_PATH/etc/default_partitions.txt
chmod 644 $INSTALL_PATH/etc/default_partitions.txt

echo " Done."
echo

echo '##############################'
echo '######  CONFIGURATIONS  ######'
echo '##############################'
echo

### Install the configs ###

# Init script

INITSCRIPT=$INSTALL_PATH/etc/profile.d/init.sh
echo "-> Writing $INITSCRIPT.."

echo "export DYNAMO_BASE=$INSTALL_PATH" > $INITSCRIPT
echo "export DYNAMO_ARCHIVE=$ARCHIVE_PATH" >> $INITSCRIPT
echo "export DYNAMO_SPOOL=$SPOOL_PATH" >> $INITSCRIPT
[ $POLICY_PATH ] && echo "export DYNAMO_POLICIES=$POLICY_PATH" >> $INITSCRIPT
echo "export PYTHONPATH="'$DYNAMO_BASE/python/site-packages:$(echo $PYTHONPATH | sed "s|$DYNAMO_BASE/python/site-packages:||")' >> $INITSCRIPT
echo "export PATH="'$DYNAMO_BASE/bin:$DYNAMO_BASE/sbin:$(echo $PATH | sed "s|$DYNAMO_BASE/bin:$DYNAMO_BASE/sbin:||")' >> $INITSCRIPT

echo " Done."
echo

# Server conf

echo "-> Writing $CONFIG_PATH/server_config.json.."

TMP=/tmp/dynamo_server_confg.tmp.$$
touch $TMP
chmod 600 $TMP

$SOURCE/sbin/dynamo-server-conf $INSTALL_CONF >> $TMP

if [ -e $CONFIG_PATH/server_config.json ]
then
  echo " File already exists."
  if ! diff $TMP $CONFIG_PATH/server_config.json > /dev/null 2>&1
  then
    echo " Difference found (existing | installation):"
    echo
    diff -y $CONFIG_PATH/server_config.json $TMP
    echo " New file saved as $CONFIG_PATH/server_config.json.new"
    echo
    mv $TMP $CONFIG_PATH/server_config.json.new
  else
    rm $TMP
  fi
else
  touch $CONFIG_PATH/server_config.json
  chmod 600 $CONFIG_PATH/server_config.json
  cat $TMP >> $CONFIG_PATH/server_config.json
  echo " Done."
  echo
fi

# The rest of the config files are copied directly - edit as necessary

echo "-> Copying other configuration files.."

for CONF in $(ls $SOURCE/config/*.json)
do
  FILE=$(basename $CONF)
  if ! [ -e $CONFIG_PATH/$FILE ]
  then
    cp $CONF $CONFIG_PATH/$FILE
  elif ! diff $SOURCE/config/$FILE $CONFIG_PATH/$FILE > /dev/null 2>&1
  then
    echo " Config $FILE has changed. Difference (old | new):"
    echo
    cp $CONF $CONFIG_PATH/$FILE.new
    diff -y $CONFIG_PATH/$FILE{,.new}
    echo " New file saved as $CONFIG_PATH/$FILE.new"
    echo
  fi
done

echo " Done."
echo

### Set up the databases ###

if [ $SERVER_DB ]
then
  $SOURCE/$SERVER_DB/install.sh
  if [ $? -ne 0 ]
  then
    echo
    echo "DB installation failed."
    exit 1
  fi
fi

### Install the web scripts ###

if [ $WEBSERVER ]
then
  require rpm -q lighttpd
  require rpm -q lighttpd-fastcgi
  if [[ $(getsebool httpd_setrlimit) =~ off ]]
  then
    setsebool httpd_setrlimit 1
  fi
fi

### Install the daemons ###

echo '########################'
echo '######  SERVICES  ######'
echo '########################'
echo
echo "-> Installing dynamod.."

if [[ $(uname -r) =~ el7 ]]
then
  # systemd daemon
  cp $SOURCE/daemon/dynamod.systemd /usr/lib/systemd/system/dynamod.service
  sed -i "s|_INSTALLPATH_|$INSTALL_PATH|" /usr/lib/systemd/system/dynamod.service

  # environment file for the daemon
  ENV=/etc/sysconfig/dynamod
  echo "DYNAMO_BASE=$INSTALL_PATH" > $ENV
  echo "DYNAMO_ARCHIVE=$ARCHIVE_PATH" >> $ENV
  echo "DYNAMO_SPOOL=$SPOOL_PATH" >> $ENV
  [ $POLICY_PATH ] && echo "DYNAMO_POLICIES=$POLICY_PATH" >> $ENV
  echo "PYTHONPATH=$INSTALL_PATH/python/site-packages" >> $ENV

  systemctl daemon-reload
else
  cp $SOURCE/daemon/dynamod.sysv /etc/init.d/dynamod
  sed -i "s|_INSTALLPATH_|$INSTALL_PATH|" /etc/init.d/dynamod
  chmod +x /etc/init.d/dynamod
fi

echo " Done."
echo

# CRONTAB
#crontab -l -u $USER > /tmp/$USER.crontab
#sed "s|_INSTALLPATH_|$INSTALL_PATH|" $SOURCE/etc/crontab >> /tmp/$USER.crontab
#sort /tmp/$USER.crontab | uniq | crontab -u $USER -
#rm /tmp/$USER.crontab

# NRPE PLUGINS
if [ -d /usr/lib64/nagios/plugins ]
then
  echo "-> Installing nagios plugin.."

  sed "s|_ARCHIVEPATH_|$ARCHIVE_PATH|" $SOURCE/etc/nrpe/check_dynamo.sh > /usr/lib64/nagios/plugins/check_dynamo.sh
  chmod +x /usr/lib64/nagios/plugins/check_dynamo.sh

  echo " Done."
  echo
fi

echo "Dynamo installation completed."
