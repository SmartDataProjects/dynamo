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

if ! [ -e $SOURCE/config.sh ]
then
  echo
  echo "$SOURCE/config.sh does not exist."
  exit 1
fi

echo
echo "Installing dynamo from $SOURCE."
echo

source $SOURCE/config.sh

if [ $DAEMONS -eq 1 ]
then
  ### Stop the daemons first

  if [[ $(uname -r) =~ el7 ]]
  then
    systemctl stop dynamod 2>/dev/null
    systemctl stop dynamo-scheduled 2>/dev/null
  else
    service dynamod stop 2>/dev/null
    service dynamo-scheduled stop 2>/dev/null
  fi
fi

### Verify required components

echo
echo "Checking dependencies.."
echo
require which python
require python -c 'import MySQLdb'
warnifnot python -c 'import htcondor'
require which mysql
require which sqlite3
if [ $WEBPATH ]
then
  require pgrep -f httpd
  require which php
  require [ -e /etc/httpd/conf.d/ssl.conf ]
  require php -r 'mysqli_connect_errno();'
fi
[ $SERVER_DB_HOST = localhost ] && require pgrep -f mysqld

### (Clear &) Make the directories ###

if [ -d $INSTALLPATH ]
then
  echo
  echo "Target directory $INSTALLPATH exists. Overwrite [y/n]?"
  if confirmed
  then
    rm -rf $INSTALLPATH
  else
    echo "Exiting."
    exit 1
  fi
fi
echo

require mkdir -p $INSTALLPATH
require mkdir -p $INSTALLPATH/python/site-packages/dynamo
require mkdir -p $INSTALLPATH/bin
require mkdir -p $INSTALLPATH/exec
require mkdir -p $INSTALLPATH/sbin
require mkdir -p $INSTALLPATH/etc/profile.d
chown -R $USER:$(id -gn $USER) $INSTALLPATH

require mkdir -p $CONFIGPATH

require mkdir -p $LOGPATH
chown $USER:$(id -gn $USER) $LOGPATH

require mkdir -p $SCHEDULERPATH
chown $USER:$(id -gn $USER) $SCHEDULERPATH

require mkdir -p $ARCHIVEPATH
chown $USER:$(id -gn $USER) $ARCHIVEPATH

mkdir -p $SPOOLPATH
chown $USER:$(id -gn $USER) $SPOOLPATH
chmod 777 $SPOOLPATH

### Install python libraries ###

cp -r $SOURCE/lib/* $INSTALLPATH/python/site-packages/dynamo/
python -m compileall $INSTALLPATH/python/site-packages/dynamo > /dev/null

### Install the executables ###

cp $SOURCE/bin/* $INSTALLPATH/bin/
chown $USER:$(id -gn $USER) $INSTALLPATH/bin/*
chmod 755 $INSTALLPATH/bin/*

cp $SOURCE/exec/* $INSTALLPATH/exec/
chown $USER:$(id -gn $USER) $INSTALLPATH/exec/*
chmod 755 $INSTALLPATH/exec/*

cp $SOURCE/sbin/* $INSTALLPATH/sbin/
chown root:$(id -gn $USER) $INSTALLPATH/sbin/*
chmod 754 $INSTALLPATH/sbin/*

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

INITSCRIPT=$INSTALLPATH/etc/profile.d/init.sh
echo "export DYNAMO_BASE=$INSTALLPATH" > $INITSCRIPT
echo "export DYNAMO_ARCHIVE=$ARCHIVEPATH" >> $INITSCRIPT
echo "export DYNAMO_SPOOL=$SPOOLPATH" >> $INITSCRIPT
echo "export DYNAMO_SPOOL=$SPOOLPATH" >> $INITSCRIPT
echo "export PYTHONPATH="'$DYNAMO_BASE/python/site-packages:$(echo $PYTHONPATH | sed "s|$DYNAMO_BASE/python/site-packages:||")' >> $INITSCRIPT
echo "export PATH="'$DYNAMO_BASE/bin:$DYNAMO_BASE/sbin:$(echo $PATH | sed "s|$DYNAMO_BASE/bin:$DYNAMO_BASE/sbin:||")' >> $INITSCRIPT

if [ -e $CONFIGPATH/server_config.json ]
then
  echo "$CONFIGPATH/server_config.json exists. Not overwriting."
else
  cp $SOURCE/config/server_config.json.template $CONFIGPATH/server_config.json

  sed -i "s|_USER_|$USER|" $CONFIGPATH/server_config.json
  sed -i "s|_LOGPATH_|$LOGPATH|" $CONFIGPATH/server_config.json
  sed -i "s|_SCHEDULER_PATH_|$SCHEDULERPATH|" $CONFIGPATH/server_config.json
  sed -i "s|_REGISTRY_HOST_|$REGISTRY_HOST|" $CONFIGPATH/server_config.json

  sed -n '1,/_SERVER_DB_WRITE_PARAMS_1_/ p' $CONFIGPATH/server_config.json | sed '$ d' > server_config.json.tmp

  echo '          "user": "'$SERVER_DB_WRITE_USER'",' >> server_config.json.tmp
  echo '          "passwd": "'$SERVER_DB_WRITE_PASSWD'",' >> server_config.json.tmp
  echo '          "host": "'$SERVER_DB_HOST'",' >> server_config.json.tmp
  echo '          "db": "'$SERVER_DB'"' >> server_config.json.tmp

  sed '/_SERVER_DB_WRITE_PARAMS_1_/,/_SERVER_DB_READ_PARAMS_1_/ !d;//d' $CONFIGPATH/server_config.json >> server_config.json.tmp

  [ $SERVER_DB_READ_CNF ] && echo '          "config_file": "'$SERVER_DB_READ_CNF'",' >> server_config.json.tmp
  [ $SERVER_DB_READ_CNFGROUP ] && echo '          "config_group": "'$SERVER_DB_READ_CNFGROUP'",' >> server_config.json.tmp
  [ $SERVER_DB_READ_USER ] && echo '          "user": "'$SERVER_DB_READ_USER'",' >> server_config.json.tmp
  [ $SERVER_DB_READ_PASSWD ] && echo '          "passwd": "'$SERVER_DB_READ_PASSWD'",' >> server_config.json.tmp
  echo '          "host": "'$SERVER_DB_HOST'",' >> server_config.json.tmp
  echo '          "db": "'$SERVER_DB'"' >> server_config.json.tmp

  sed '/_SERVER_DB_READ_PARAMS_1_/,/_SERVER_DB_WRITE_PARAMS_2_/ !d;//d' $CONFIGPATH/server_config.json >> server_config.json.tmp

  echo '        "user": "'$SERVER_DB_WRITE_USER'",' >> server_config.json.tmp
  echo '        "passwd": "'$SERVER_DB_WRITE_PASSWD'",' >> server_config.json.tmp
  echo '        "host": "'$SERVER_DB_HOST'",' >> server_config.json.tmp
  echo '        "db": "'$REGISTRY_DB'"' >> server_config.json.tmp

  sed '/_SERVER_DB_WRITE_PARAMS_2_/,/_SERVER_DB_READ_PARAMS_2_/ !d;//d' $CONFIGPATH/server_config.json >> server_config.json.tmp

  [ $SERVER_DB_READ_CNF ] && echo '        "config_file": "'$SERVER_DB_READ_CNF'",' >> server_config.json.tmp
  [ $SERVER_DB_READ_CNFGROUP ] && echo '        "config_group": "'$SERVER_DB_READ_CNFGROUP'",' >> server_config.json.tmp
  [ $SERVER_DB_READ_USER ] && echo '        "user": "'$SERVER_DB_READ_USER'",' >> server_config.json.tmp
  [ $SERVER_DB_READ_PASSWD ] && echo '        "passwd": "'$SERVER_DB_READ_PASSWD'",' >> server_config.json.tmp
  echo '        "host": "'$SERVER_DB_HOST'",' >> server_config.json.tmp
  echo '        "db": "'$REGISTRY_DB'"' >> server_config.json.tmp

  sed '/_SERVER_DB_READ_PARAMS_2_/,$ !d;//d' $CONFIGPATH/server_config.json >> server_config.json.tmp
  
  mv server_config.json.tmp $CONFIGPATH/server_config.json
fi

chmod 600 $CONFIGPATH/server_config.json

# The rest of the config files are copied directly - edit as necessary
for CONF in $(ls $SOURCE/config/*.json)
do
  FILE=$(basename $CONF)
  if ! [ -e $CONFIGPATH/$FILE ]
  then
    cp $CONF $CONFIGPATH/$FILE
  elif ! diff $SOURCE/config/$FILE $CONFIGPATH/$FILE > /dev/null 2>&1
  then
    echo "Config $FILE has changed. Saving the new file to $CONFIGPATH/$FILE.new."
    echo
    cp $CONF $CONFIGPATH/$FILE.new
  fi
done

### Install the policies ###

echo
echo "Installing the policies."
echo

TAG=$(cat $SOURCE/etc/policies.tag)
git clone -b branch-v2.0 https://github.com/yiiyama/dynamo-policies.git $INSTALLPATH/policies
#git clone https://github.com/SmartDataProjects/dynamo-policies.git $INSTALLPATH/policies
#cd $INSTALLPATH/policies
#git checkout $TAG >/dev/null 2>&1
#echo "Policy commit:"
#git log -1
#cd - > /dev/null

### Install the web scripts ###

if [ $WEBPATH ]
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
    sed -i "s|_INSTALLPATH_|$INSTALLPATH|" /usr/lib/systemd/system/dynamod.service

    cp $SOURCE/daemon/dynamo-scheduled.systemd /usr/lib/systemd/system/dynamo-scheduled.service
    sed -i "s|_INSTALLPATH_|$INSTALLPATH|" /usr/lib/systemd/system/dynamo-scheduled.service

    # environment file for the daemon
    echo "DYNAMO_BASE=$INSTALLPATH" > /etc/sysconfig/dynamod
    echo "DYNAMO_ARCHIVE=$ARCHIVEPATH" >> /etc/sysconfig/dynamod
    echo "DYNAMO_SPOOL=$SPOOLPATH" >> /etc/sysconfig/dynamod
    echo "DYNAMO_SPOOL=$SPOOLPATH" >> /etc/sysconfig/dynamod
    echo "PYTHONPATH=$INSTALLPATH/python/site-packages" >> /etc/sysconfig/dynamod

    systemctl daemon-reload
  else
    cp $SOURCE/daemon/dynamod.sysv /etc/init.d/dynamod
    sed -i "s|_INSTALLPATH_|$INSTALLPATH|" /etc/init.d/dynamod
    chmod +x /etc/init.d/dynamod

    cp $SOURCE/daemon/dynamo-scheduled.sysv /etc/init.d/dynamo-scheduled
    sed -i "s|_INSTALLPATH_|$INSTALLPATH|" /etc/init.d/dynamo-scheduled
    chmod +x /etc/init.d/dynamo-scheduled
  fi

  # CRONTAB
  crontab -l -u $USER > /tmp/$USER.crontab
  sed "s|_INSTALLPATH_|$INSTALLPATH|" $SOURCE/etc/crontab >> /tmp/$USER.crontab
  sort /tmp/$USER.crontab | uniq | crontab -u $USER -
  rm /tmp/$USER.crontab

  # NRPE PLUGINS
  if [ -d /usr/lib64/nagios/plugins ]
  then
    sed "s|_ARCHIVEPATH_|$ARCHIVEPATH|" $SOURCE/etc/nrpe/check_dynamo.sh > /usr/lib64/nagios/plugins/check_dynamo.sh
    chmod +x /usr/lib64/nagios/plugins/check_dynamo.sh
  fi
fi
