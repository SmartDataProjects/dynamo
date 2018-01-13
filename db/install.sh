#!/bin/bash

echo "Setting up MySQL databases."

if ! [ $SOURCE ]
then
  echo "Install source path is not set."
  exit 1
fi

MYSQLOPT="-u $SERVER_DB_WRITE_USER -p$SERVER_DB_WRITE_PASSWD -h localhost"

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
