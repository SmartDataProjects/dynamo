#!/bin/bash

DB=$1

INBLOCK=0
OPTIONS=
while read LINE
do
  if [ "$LINE" = "[mysql-dynamo]" ]
  then
    INBLOCK=1
    continue
  fi

  [ $INBLOCK -eq 0 ] && continue

  KEY=$(echo $LINE | cut -d'=' -f 1)
  VALUE=$(echo $LINE | cut -d'=' -f 2)

  case $KEY in
    host)
      OPTIONS="$OPTIONS -h $VALUE"
      ;;
    user)
      OPTIONS="$OPTIONS -u $VALUE"
      ;;
    password)
      OPTIONS="$OPTIONS -p$VALUE"
      ;;
  esac
done < /etc/my.cnf

mysqldump $OPTIONS $DB -d $ARGS | sed 's/AUTO_INCREMENT=[0-9]*/AUTO_INCREMENT=1/' > $DB.sql
