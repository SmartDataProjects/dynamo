#!/bin/bash

confirmed () {
  while true
  do
    read RESPONSE
    case $RESPONSE in
      y)
        return 0
        ;;
      n)
        return 1
        ;;
      *)
        echo "Please answer in y/n."
        ;;
    esac
  done
}

require () {
  "$@" >/dev/null 2>&1 && return 0
  echo
  echo "[Fatal] Failed: $@"
  exit 1
}

warnifnot () {
  "$@" >/dev/null 2>&1 && return 0
  echo
  echo "[Warning] Failed: $@"
  echo "Some components may not work."
}

### Where we are installing from (i.e. this directory) ###

export SOURCE=$(cd $(dirname ${BASH_SOURCE[0]}); pwd)

### Read the config ###

if ! [ -e $SOURCE/config.sh ]
then
  echo
  echo "$SOURCE/config.sh does not exist."
  exit 1
fi

source $SOURCE/config.sh

if [ $DAEMONS -eq 1 ]
then
  ### Stop the daemons first ###

  if [[ $(uname -r) =~ el7 ]]
  then
    systemctl stop dynamod 2>/dev/null
  else
    service dynamod stop 2>/dev/null
  fi
fi

echo
echo "Installing dynamo from $SOURCE."
echo

### Verify required components ###

echo
echo "Checking dependencies.."
echo
require which python
require python -c 'import MySQLdb'
warnifnot python -c 'import htcondor'
require which mysql
require which sqlite3
if [ $WEB_PATH ]
then
  require pgrep -f httpd
  require which php
  require [ -e /etc/httpd/conf.d/ssl.conf ]
  require php -r 'mysqli_connect_errno();'
fi
[ $SERVER_DB_HOST = localhost ] && require pgrep -f mysqld

### (Clear &) Make the directories ###

if [ -d $INSTALL_PATH ]
then
  echo
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
require mkdir -p $INSTALL_PATH/sbin
require mkdir -p $INSTALL_PATH/etc/profile.d
chown -R $USER:$(id -gn $USER) $INSTALL_PATH

require mkdir -p $CONFIG_PATH

require mkdir -p $LOG_PATH
chown $USER:$(id -gn $USER) $LOG_PATH

require mkdir -p $SCHEDULER_PATH
chown $USER:$(id -gn $USER) $SCHEDULER_PATH

require mkdir -p $ARCHIVE_PATH
chown $USER:$(id -gn $USER) $ARCHIVE_PATH

mkdir -p $SPOOL_PATH
chown $USER:$(id -gn $USER) $SPOOL_PATH
chmod 777 $SPOOL_PATH

### Install python libraries ###

cp -r $SOURCE/lib/* $INSTALL_PATH/python/site-packages/dynamo/
python -m compileall $INSTALL_PATH/python/site-packages/dynamo > /dev/null

### Install the executables ###

cp $SOURCE/bin/* $INSTALL_PATH/bin/
chown $USER:$(id -gn $USER) $INSTALL_PATH/bin/*
chmod 755 $INSTALL_PATH/bin/*

cp $SOURCE/exec/* $INSTALL_PATH/exec/
chown $USER:$(id -gn $USER) $INSTALL_PATH/exec/*
chmod 755 $INSTALL_PATH/exec/*

cp $SOURCE/sbin/* $INSTALL_PATH/sbin/
chown root:$(id -gn $USER) $INSTALL_PATH/sbin/*
chmod 754 $INSTALL_PATH/sbin/*

### Set up the databases ###

if [ "$SERVER_DB_HOST" = "localhost" ]
then
  export SERVER_DB_WRITE_USER
  export SERVER_DB_WRITE_PASSWD

  $SOURCE/db/install.sh
  if [ $? -ne 0 ]
  then
    echo
    echo "DB configuration failed."
    exit 1
  fi
fi  

### Install the configs ###

echo
echo "Writing configuration files."
echo

INITSCRIPT=$INSTALL_PATH/etc/profile.d/init.sh
echo "export DYNAMO_BASE=$INSTALL_PATH" > $INITSCRIPT
echo "export DYNAMO_ARCHIVE=$ARCHIVE_PATH" >> $INITSCRIPT
echo "export DYNAMO_SPOOL=$SPOOL_PATH" >> $INITSCRIPT
echo "export DYNAMO_SPOOL=$SPOOL_PATH" >> $INITSCRIPT
echo "export PYTHONPATH="'$DYNAMO_BASE/python/site-packages:$(echo $PYTHONPATH | sed "s|$DYNAMO_BASE/python/site-packages:||")' >> $INITSCRIPT
echo "export PATH="'$DYNAMO_BASE/bin:$DYNAMO_BASE/sbin:$(echo $PATH | sed "s|$DYNAMO_BASE/bin:$DYNAMO_BASE/sbin:||")' >> $INITSCRIPT

if [ -e $CONFIG_PATH/server_config.json ]
then
  echo "$CONFIG_PATH/server_config.json exists. Not overwriting."
else
  cp $SOURCE/config/server_config.json.template $CONFIG_PATH/server_config.json

  sed -i "s|_USER_|$USER|" $CONFIG_PATH/server_config.json
  sed -i "s|_READUSER_|$READ_USER|" $CONFIG_PATH/server_config.json
  sed -i "s|_SCHEDULERUSER_|$SCHEDULER_USER|" $CONFIG_PATH/server_config.json
  sed -i "s|_LOGPATH_|$LOG_PATH|" $CONFIG_PATH/server_config.json
  sed -i "s|_SCHEDULERPATH_|$SCHEDULER_PATH|" $CONFIG_PATH/server_config.json
  sed -i "s|_REGISTRYHOST_|$REGISTRY_HOST|" $CONFIG_PATH/server_config.json

  sed -n '1,/_SERVER_DB_WRITE_PARAMS_1_/ p' $CONFIG_PATH/server_config.json | sed '$ d' > server_config.json.tmp

  echo '          "user": "'$SERVER_DB_WRITE_USER'",' >> server_config.json.tmp
  echo '          "passwd": "'$SERVER_DB_WRITE_PASSWD'",' >> server_config.json.tmp
  echo '          "host": "'$SERVER_DB_HOST'",' >> server_config.json.tmp

  sed '/_SERVER_DB_WRITE_PARAMS_1_/,/_SERVER_DB_READ_PARAMS_1_/ !d;//d' $CONFIG_PATH/server_config.json >> server_config.json.tmp

  [ $SERVER_DB_READ_CNF ] && echo '          "config_file": "'$SERVER_DB_READ_CNF'",' >> server_config.json.tmp
  [ $SERVER_DB_READ_CNFGROUP ] && echo '          "config_group": "'$SERVER_DB_READ_CNFGROUP'",' >> server_config.json.tmp
  [ $SERVER_DB_READ_USER ] && echo '          "user": "'$SERVER_DB_READ_USER'",' >> server_config.json.tmp
  [ $SERVER_DB_READ_PASSWD ] && echo '          "passwd": "'$SERVER_DB_READ_PASSWD'",' >> server_config.json.tmp
  echo '          "host": "'$SERVER_DB_HOST'",' >> server_config.json.tmp

  sed '/_SERVER_DB_READ_PARAMS_1_/,/_SERVER_DB_WRITE_PARAMS_2_/ !d;//d' $CONFIG_PATH/server_config.json >> server_config.json.tmp

  echo '        "user": "'$SERVER_DB_WRITE_USER'",' >> server_config.json.tmp
  echo '        "passwd": "'$SERVER_DB_WRITE_PASSWD'",' >> server_config.json.tmp
  echo '        "host": "'$SERVER_DB_HOST'",' >> server_config.json.tmp

  sed '/_SERVER_DB_WRITE_PARAMS_2_/,/_SERVER_DB_READ_PARAMS_2_/ !d;//d' $CONFIG_PATH/server_config.json >> server_config.json.tmp

  [ $SERVER_DB_READ_CNF ] && echo '        "config_file": "'$SERVER_DB_READ_CNF'",' >> server_config.json.tmp
  [ $SERVER_DB_READ_CNFGROUP ] && echo '        "config_group": "'$SERVER_DB_READ_CNFGROUP'",' >> server_config.json.tmp
  [ $SERVER_DB_READ_USER ] && echo '        "user": "'$SERVER_DB_READ_USER'",' >> server_config.json.tmp
  [ $SERVER_DB_READ_PASSWD ] && echo '        "passwd": "'$SERVER_DB_READ_PASSWD'",' >> server_config.json.tmp
  echo '        "host": "'$SERVER_DB_HOST'",' >> server_config.json.tmp

  sed '/_SERVER_DB_READ_PARAMS_2_/,$ !d;//d' $CONFIG_PATH/server_config.json >> server_config.json.tmp
  
  mv server_config.json.tmp $CONFIG_PATH/server_config.json
fi

chmod 600 $CONFIG_PATH/server_config.json

# The rest of the config files are copied directly - edit as necessary
for CONF in $(ls $SOURCE/config/*.json)
do
  FILE=$(basename $CONF)
  if ! [ -e $CONFIG_PATH/$FILE ]
  then
    cp $CONF $CONFIG_PATH/$FILE
  elif ! diff $SOURCE/config/$FILE $CONFIG_PATH/$FILE > /dev/null 2>&1
  then
    echo "Config $FILE has changed. Saving the new file to $CONFIG_PATH/$FILE.new."
    echo
    cp $CONF $CONFIG_PATH/$FILE.new
  fi
done

### Install the policies ###

echo
echo "Installing the policies."
echo

TAG=$(cat $SOURCE/etc/policies.tag)
git clone -b branch-v2.0 https://github.com/yiiyama/dynamo-policies.git $INSTALL_PATH/policies
#git clone https://github.com/SmartDataProjects/dynamo-policies.git $INSTALL_PATH/policies
#cd $INSTALL_PATH/policies
#git checkout $TAG >/dev/null 2>&1
#echo "Policy commit:"
#git log -1
#cd - > /dev/null

### Install the web scripts ###

if [ $WEB_PATH ]
then
  export WEBPATH
  $SOURCE/web/install.sh
  if [ $? -ne 0 ]
  then
    echo
    echo "Web configuration failed."
    exit 1
  fi
fi

### Install the daemons ###

if [ $DAEMONS -eq 1 ]
then
  echo
  echo "Installing the daemons."

  if [[ $(uname -r) =~ el7 ]]
  then
    # systemd daemon
    cp $SOURCE/daemon/dynamod.systemd /usr/lib/systemd/system/dynamod.service
    sed -i "s|_INSTALLPATH_|$INSTALL_PATH|" /usr/lib/systemd/system/dynamod.service

    cp $SOURCE/daemon/dynamo-scheduled.systemd /usr/lib/systemd/system/dynamo-scheduled.service
    sed -i "s|_INSTALLPATH_|$INSTALL_PATH|" /usr/lib/systemd/system/dynamo-scheduled.service

    # environment file for the daemon
    echo "DYNAMO_BASE=$INSTALL_PATH" > /etc/sysconfig/dynamod
    echo "DYNAMO_ARCHIVE=$ARCHIVE_PATH" >> /etc/sysconfig/dynamod
    echo "DYNAMO_SPOOL=$SPOOL_PATH" >> /etc/sysconfig/dynamod
    echo "DYNAMO_SPOOL=$SPOOL_PATH" >> /etc/sysconfig/dynamod
    echo "PYTHONPATH=$INSTALL_PATH/python/site-packages" >> /etc/sysconfig/dynamod

    systemctl daemon-reload
  else
    cp $SOURCE/daemon/dynamod.sysv /etc/init.d/dynamod
    sed -i "s|_INSTALLPATH_|$INSTALL_PATH|" /etc/init.d/dynamod
    chmod +x /etc/init.d/dynamod

    cp $SOURCE/daemon/dynamo-scheduled.sysv /etc/init.d/dynamo-scheduled
    sed -i "s|_INSTALLPATH_|$INSTALL_PATH|" /etc/init.d/dynamo-scheduled
    chmod +x /etc/init.d/dynamo-scheduled
  fi

  # CRONTAB
  crontab -l -u $USER > /tmp/$USER.crontab
  sed "s|_INSTALLPATH_|$INSTALL_PATH|" $SOURCE/etc/crontab >> /tmp/$USER.crontab
  sort /tmp/$USER.crontab | uniq | crontab -u $USER -
  rm /tmp/$USER.crontab

  # NRPE PLUGINS
  if [ -d /usr/lib64/nagios/plugins ]
  then
    sed "s|_ARCHIVEPATH_|$ARCHIVE_PATH|" $SOURCE/etc/nrpe/check_dynamo.sh > /usr/lib64/nagios/plugins/check_dynamo.sh
    chmod +x /usr/lib64/nagios/plugins/check_dynamo.sh
  fi
fi

echo
echo "Dynamo installation completed."
