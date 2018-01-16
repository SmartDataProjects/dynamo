#!/bin/bash

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

if ! [ -d $INSTALL_PATH ]
then
  echo
  echo "Install Dynamo with install.sh first."
  exit 1
fi

echo
echo "Installing dynamo scheduler from $SOURCE."
echo

if [ $DAEMONS -eq 1 ]
then
  ### Stop the daemons first ###

  if [[ $(uname -r) =~ el7 ]]
  then
    systemctl stop dynamo-scheduled 2>/dev/null
  else
    service dynamo-scheduled stop 2>/dev/null
  fi
fi

### Copy the scheduler sequence file

cp $SOURCE/schedule/$SCHEDULER_SEQ /etc/dynamo/scheduled.seq

### Check the registry user and authorize the executables

REGSQL="mysql -u $SERVER_DB_WRITE_USER -p$SERVER_DB_WRITE_PASSWD -h $REGISTRY_HOST -D dynamoregister"

REGID=$(echo 'SELECT `id` FROM `users` WHERE `name` = "'$SCHEDULER_USER'";' | $REGSQL)
if [ "$REGID" ]
then
  echo "User $SCHEDULER_USER is not in Dynamo registry."
  exit 1
fi

source $INSTALL_PATH/etc/profile.d/init.sh

while read LINE
do
  [[ $LINE =~ ^\[SEQUENCE ]] && break

  TITLE=$(echo $LINE | sed -n 's|<\(.*\)> *= *.*|\1|p')
  [ "$TITLE" ] || continue

  EXEC=$(echo $LINE | sed -n 's|<.*> *= *\(.*\)|\1|p')
  [ "$EXEC" ] || continue

  EXEC=$(echo $EXEC | sed 's|\$(DYNAMO_BASE)|'$INSTALL_PATH'|')
  if ! [ -e $EXEC ]
  then
    echo "Executable $EXEC not found. Cannot add authorization."
    continue
  fi

  dynamo-exec-auth --executable $EXEC --user $SCHEDULER_USER --title $TITLE

done < $SOURCE/schedule/$SCHEDULER_SEQ
