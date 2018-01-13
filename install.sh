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
  echo "[Fatal] Failed: $@"
  exit 1
}

warnifnot () {
  "$@" >/dev/null 2>&1 && return 0
  echo "[Warning] Failed: $@"
  echo "Some components may not work."
}

### Where we are installing from (i.e. this directory) ###

export SOURCE=$(cd $(dirname ${BASH_SOURCE[0]}); pwd)

echo "Installing dynamo from $SOURCE."

source $SOURCE/config.sh

### Verify required components

echo "Checking dependencies.."
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
  echo "Target directory $INSTALLPATH exists. Overwrite [y/n]?"
  if confirmed
  then
    rm -rf $INSTALLPATH
  else
    echo "Exiting."
    exit 1
  fi
fi

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
python -m compileall $INSTALLPATH/python/site-packages/dynamo

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
  export SERVER_DB_WRITE_CNF
  export SERVER_DB_WRITE_CNFGROUP
  export SERVER_DB_WRITE_USER
  export SERVER_DB_WRITE_PASSWD
  
  $SOURCE/db/install.sh
  if [ $? -ne 0 ]
  then
    echo "DB configuration failed."
    exit 1
  fi
fi  

### Install the configs ###

echo "Writing configuration files."

echo "export DYNAMO_BASE=$INSTALLPATH" > $INSTALLPATH/etc/profile.d/init.sh
echo "export DYNAMO_ARCHIVE=$ARCHIVEPATH" >> $INSTALLPATH/etc/profile.d/init.sh
echo "export DYNAMO_SPOOL=$SPOOLPATH" >> $INSTALLPATH/etc/profile.d/init.sh
echo "export DYNAMO_SPOOL=$SPOOLPATH" >> $INSTALLPATH/etc/profile.d/init.sh
echo "export PYTHONPATH="'$DYNAMO_BASE/python/site-packages:$(echo $PYTHONPATH | sed "s|$DYNAMO_BASE/python/site-packages:||")' >> $INSTALLPATH/etc/profile.d/init.sh

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

  [ $SERVER_DB_WRITE_CNF ] && echo '          "config_file": "'$SERVER_DB_WRITE_CNF'",' >> server_config.json.tmp
  [ $SERVER_DB_WRITE_CNFGROUP ] && echo '          "config_group": "'$SERVER_DB_WRITE_CNFGROUP'",' >> server_config.json.tmp
  [ $SERVER_DB_WRITE_USER ] && echo '          "user": "'$SERVER_DB_WRITE_USER'",' >> server_config.json.tmp
  [ $SERVER_DB_WRITE_PASSWD ] && echo '          "passwd": "'$SERVER_DB_WRITE_PASSWD'",' >> server_config.json.tmp
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

  [ $SERVER_DB_WRITE_CNF ] && echo '        "config_file": "'$SERVER_DB_WRITE_CNF'",' >> server_config.json.tmp
  [ $SERVER_DB_WRITE_CNFGROUP ] && echo '        "config_group": "'$SERVER_DB_WRITE_CNFGROUP'",' >> server_config.json.tmp
  [ $SERVER_DB_WRITE_USER ] && echo '        "user": "'$SERVER_DB_WRITE_USER'",' >> server_config.json.tmp
  [ $SERVER_DB_WRITE_PASSWD ] && echo '        "passwd": "'$SERVER_DB_WRITE_PASSWD'",' >> server_config.json.tmp
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
  if [ -e $CONFIGPATH/$(basename $CONF) ]
  then
    cp $CONF $CONFIGPATH/$(basename $CONF).new
  else
    cp $CONF $CONFIGPATH/$(basename $CONF)
  fi
done

### Install the policies ###

echo "Installing the policies."

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
    echo "Web configuration failed."
    exit 1
  fi
fi

### Install the daemons ###

if [ $DAEMONS -eq 1 ]
then
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
