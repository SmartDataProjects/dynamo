#!/bin/bash

### Where we are installing from (i.e. this directory) ###

export SOURCE=$(cd $(dirname ${BASH_SOURCE[0]}); pwd)

### Read the config ###

if ! [ -e $SOURCE/dynamo.cfg ]
then
  echo
  echo "$SOURCE/dynamo.cfg does not exist."
  exit 1
fi

source $SOURCE/utilities/shellutils.sh

READCONF="$SOURCE/utilities/readconf -I $SOURCE/dynamo.cfg"

USER=$($READCONF server.user)
DYNAMO_BASE=$($READCONF paths.dynamo_base)
CONFIG_PATH=$($READCONF paths.config_path)
WORKDIR=$($READCONF scheduler.workdir)
SEQUENCE=$($READCONF scheduler.sequence)
SCHED_USER=$($READCONF scheduler.user)

echo
echo "Installing dynamo scheduler from $SOURCE."
echo

### Stop the daemons first ###

if [[ $(uname -r) =~ el7 ]]
then
  systemctl stop dynamo-scheduled 2>/dev/null
else
  service dynamo-scheduled stop 2>/dev/null
fi

### Make working directories ###

require mkdir -p $WORKDIR
chown $USER:$(id -gn $USER) $WORKDIR

### Copy the scheduler sequence file ###

cp $SOURCE/schedule/$SEQUENCE $CONFIG_PATH/scheduled.seq

source $DYNAMO_BASE/etc/profile.d/init.sh

while read LINE
do
  [[ $LINE =~ ^\[SEQUENCE ]] && break

  TITLE=$(echo $LINE | sed -n 's|<\(.*\)> *= *.*|\1|p')
  [ "$TITLE" ] || continue

  EXEC=$(echo $LINE | sed -n 's|<.*> *= *\(.*\)|\1|p')
  [ "$EXEC" ] || continue

  EXEC=$(echo $EXEC | sed 's|\$(DYNAMO_BASE)|'$DYNAMO_BASE'|')
  if ! [ -e $EXEC ]
  then
    echo "Executable $EXEC not found. Cannot add authorization."
    continue
  fi

  dynamo-exec-auth --executable $EXEC --user $SCHED_USER --title $TITLE

done < $SOURCE/schedule/$SEQUENCE

echo
echo "Installing the daemon."

if [[ $(uname -r) =~ el7 ]]
then
  cp $SOURCE/daemon/dynamo-scheduled.systemd /usr/lib/systemd/system/dynamo-scheduled.service
  sed -i "s|_INSTALLPATH_|$INSTALL_PATH|" /usr/lib/systemd/system/dynamo-scheduled.service
  sed -i "s|_WORKDIR_|$WORKDIR|" /usr/lib/systemd/system/dynamo-scheduled.service
  sed -i "s|_USER_|$SCHED_USER|" /usr/lib/systemd/system/dynamo-scheduled.service

  systemctl daemon-reload
else
  cp $SOURCE/daemon/dynamo-scheduled.sysv /etc/init.d/dynamo-scheduled
  sed -i "s|_INSTALLPATH_|$INSTALL_PATH|" /etc/init.d/dynamo-scheduled
  sed -i "s|_WORKDIR_|$WORKDIR|" /etc/init.d/dynamo-scheduled
  sed -i "s|_USER_|$SCHED_USER|" /etc/init.d/dynamo-scheduled
  chmod +x /etc/init.d/dynamo-scheduled
fi
