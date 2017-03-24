#!/bin/bash

USER=$1

if ! [ $USER ]
then
  echo "Using cmsprod as the user of the executables."
  USER=cmsprod
fi

export DYNAMO_BASE=$(cd $(dirname ${BASH_SOURCE[0]}); pwd)
source $DYNAMO_BASE/etc/profile.d/init.sh

# DAEMONS
sed -e "s|_DYNAMO_BASE_|$DYNAMO_BASE|" -e "s|_USER_|$USER|" $DYNAMO_BASE/sysv/dynamo-detoxd > /etc/init.d/dynamo-detoxd
sed -e "s|_DYNAMO_BASE_|$DYNAMO_BASE|" -e "s|_USER_|$USER|" $DYNAMO_BASE/sysv/dynamo-dealerd > /etc/init.d/dynamo-dealerd
sed -e "s|_DYNAMO_BASE_|$DYNAMO_BASE|" -e "s|_USER_|$USER|" $DYNAMO_BASE/sysv/dynamod > /etc/init.d/dynamod
chmod +x /etc/init.d/dynamo-detoxd
chmod +x /etc/init.d/dynamo-dealerd
chmod +x /etc/init.d/dynamod

# DIRECTORIES
mkdir -p $DYNAMO_LOGDIR
chmod 775 $DYNAMO_LOGDIR
chown root:$(id -gn $USER) $DYNAMO_LOGDIR

mkdir -p $DYNAMO_DATADIR
chmod 775 $DYNAMO_DATADIR
chown root:$(id -gn $USER) $DYNAMO_DATADIR

# WEB INTERFACE
$DYNAMO_BASE/web/install.sh

# POLICIES
[ -e $DYNAMO_BASE/policies ] || git clone https://github.com/SmartDataProjects/dynamo-policies.git $DYNAMO_BASE/policies

# NRPE PLUGINS
if [ -d /usr/lib64/nagios/plugins ]
then
  sed "s|_DYNAMO_BACKUP_PATH_|$DYNAMO_BACKUP_PATH|" $DYNAMO_BASE/etc/nrpe/check_dynamo.sh > /usr/lib64/nagios/plugins/check_dynamo.sh
  chmod +x /usr/lib64/nagios/plugins/check_dynamo.sh
fi

cd $DYNAMO_BASE/policies
TAG=$(cat $DYNAMO_BASE/etc/policies.tag)
echo "Checking out policies tag $TAG"
git checkout master
git pull origin
git checkout $TAG 2> /dev/null
cd - > /dev/null

# CRONTAB
crontab -l -u $USER > /tmp/$USER.crontab
sed "s|_DYNAMO_BASE_|$DYNAMO_BASE|" $DYNAMO_BASE/etc/crontab >> /tmp/$USER.crontab
sort /tmp/$USER.crontab | uniq | crontab -u $USER -
rm /tmp/$USER.crontab
