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

cd $DYNAMO_BASE/policies
TAG=$(cat $DYNAMO_BASE/etc/policies.tag)
echo "Checking out policies tag $TAG"
git checkout master
git pull origin
git checkout $TAG 2> /dev/null
cd - > /dev/null

# CRONTAB
sed "s|_DYNAMO_BASE_|$DYNAMO_BASE|" $DYNAMO_BASE/etc/crontab | crontab -u $USER -
