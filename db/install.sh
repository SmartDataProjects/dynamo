#!/bin/bash

echo "Setting up MySQL databases."

if ! [ $SOURCE ]
then
  echo "Install source path is not set."
  exit 1
fi

if [ $SERVER_DB_WRITE_CNF ] && [ $SERVER_DB_WRITE_CNFGROUP ]
then
  MYSQLOPT="--defaults-file=$SERVER_DB_WRITE_CNF"
  SUFFIX=$(echo $SERVER_DB_WRITE_CNFGROUP | sed 's/^mysql//')
  [ $SUFFIX ] && MYSQLOPT=$MYSQLOPT" --defaults-group-suffix=$SUFFIX"
elif [ $SERVER_DB_WRITE_USER ] && [ $SERVER_DB_WRITE_PASSWD ]
then
  MYSQLOPT="-u $SERVER_DB_WRITE_USER -p$SERVER_DB_WRITE_PASSWD"
fi

MYSQLOPT=$MYSQLOPT" -h localhost"

# Check user validity
echo "SELECT 1;" | mysql $MYSQLOPT >/dev/null 2>&1
if [ $? -ne 0 ]
then
  echo "MySQL user permission is not set."
  exit 1
fi

mkdir .tmp
cd .tmp

for SCHEMA in $(ls $SOURCE/db | grep '\.sql$')
do
  DB=$(echo $SCHEMA | sed 's/\.sql$//')
  $SOURCE/db/mysqldump.sh $MYSQLOPT $DB

  # mysqldump.sh does not create a file if the DB does not exist
  if [ -e $DB.sql ]
  then
    diff $DB.sql $SOURCE/db/$SCHEMA > /dev/null 2>&1
    if [ $? -ne 0 ]
    then
      echo "Differences were found in schema for database $DB."
      echo "Please manually update the schema."
    fi
  else
    echo "CREATE DATABASE $DB;" | mysql $MYSQLOPT
    mysql $MYSQLOPT -D $DB < $SOURCE/db/$SCHEMA
  fi
done

cd ..
rm -rf .tmp
