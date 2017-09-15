
#!/bin/bash

export USER=$1

if ! [ $USER ]
then
  echo "Usage: install.sh <user> [production]"
  exit 1
fi

if [ "$2" = "production" ]
then
  PRODUCTION=1
fi

export DYNAMO_BASE=$(cd $(dirname ${BASH_SOURCE[0]}); pwd)
source $DYNAMO_BASE/etc/profile.d/init.sh

# DIRECTORIES
mkdir -p $DYNAMO_LOGDIR
chmod 775 $DYNAMO_LOGDIR
chown root:$(id -gn $USER) $DYNAMO_LOGDIR

mkdir -p $DYNAMO_ARCHIVE
chmod 775 $DYNAMO_ARCHIVE
chown $USER:$(id -gn $USER) $DYNAMO_ARCHIVE
mkdir -p $DYNAMO_ARCHIVE/db
mkdir -p $DYNAMO_ARCHIVE/replica_snapshots

mkdir -p $DYNAMO_SPOOL
chmod 777 $DYNAMO_SPOOL

# DATABASES
for SQL in $(ls $DYNAMO_BASE/etc/db)
do
  DB=$(echo $SQL | sed 's/.sql$//')
  if ! [ -d /var/lib/mysql/$DB ]
  then
    mysql --default-group-suffix=-dynamo < $DYNAMO_BASE/etc/db/$SQL
  fi

  chmod 755 /var/lib/mysql/$DB
  chmod 666 /var/lib/mysql/$DB/*
done

# WEB INTERFACE
$DYNAMO_BASE/web/install.sh

# POLICIES
[ -e $DYNAMO_BASE/policies ] || git clone https://github.com/SmartDataProjects/dynamo-policies.git $DYNAMO_BASE/policies

cd $DYNAMO_BASE/policies
TAG=$(cat $DYNAMO_BASE/etc/policies.tag)
echo "Checking out policies tag $TAG"
git checkout master
git pull origin
git checkout $TAG 2> /dev/null
cd - > /dev/null

if [ $PRODUCTION ]
then
  # DAEMON
  sed -e "s|_DYNAMO_BASE_|$DYNAMO_BASE|" -e "s|_USER_|$USER|" $DYNAMO_BASE/sysv/dynamod > /etc/init.d/dynamod
  chmod +x /etc/init.d/dynamod

  # CRONTAB
  crontab -l -u $USER > /tmp/$USER.crontab
  sed "s|_DYNAMO_BASE_|$DYNAMO_BASE|" $DYNAMO_BASE/etc/crontab >> /tmp/$USER.crontab
  sort /tmp/$USER.crontab | uniq | crontab -u $USER -
  rm /tmp/$USER.crontab

  # NRPE PLUGINS
  if [ -d /usr/lib64/nagios/plugins ]
  then
    sed "s|_DYNAMO_ARCHIVE_|$DYNAMO_ARCHIVE|" $DYNAMO_BASE/etc/nrpe/check_dynamo.sh > /usr/lib64/nagios/plugins/check_dynamo.sh
    chmod +x /usr/lib64/nagios/plugins/check_dynamo.sh
  fi
fi
