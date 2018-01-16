#!/bin/bash

###########################################################################################
## install.sh
##
## Sets up MySQL databases. Can be run even after the DB is set up (will not overwrite).
## Compares the schema in the repository with the existing; if differences are found,
## prints a warning but does not do anything.
###########################################################################################

echo
echo "Setting up MySQL databases."
echo

require () {
  "$@" >/dev/null 2>&1 && return 0
  echo
  echo "[Fatal] Failed: $@"
  exit 1
}

### Source the configuration parameters

export SOURCE=$(cd $(dirname ${BASH_SOURCE[0]})/..; pwd)

if ! [ -e $SOURCE/config.sh ]
then
  echo
  echo "$SOURCE/config.sh does not exist."
  exit 1
fi

require source $SOURCE/config.sh

MYSQLOPT="-u $SERVER_DB_WRITE_USER -p$SERVER_DB_WRITE_PASSWD -h localhost"

# Check user validity
echo "SELECT 1;" | mysql $MYSQLOPT >/dev/null 2>&1
if [ $? -ne 0 ]
then
  echo
  echo "MySQL user permission is not set. Run db/grants.sh first."
  exit 1
fi

require mkdir .tmp
cd .tmp

for SCHEMA in $(ls $SOURCE/db | grep '\.sql$')
do
  DB=$(echo $SCHEMA | sed 's/\.sql$//')
  require $SOURCE/db/mysqldump.sh $MYSQLOPT $DB

  # mysqldump.sh does not create a file if the DB does not exist
  if [ -e $DB.sql ]
  then
    diff $DB.sql $SOURCE/db/$SCHEMA > /dev/null 2>&1
    if [ $? -ne 0 ]
    then
      echo
      echo "Differences were found in schema for database $DB."
      echo "Please manually update the schema."
      echo
    fi
  else
    echo "CREATE DATABASE $DB;" | mysql $MYSQLOPT
    mysql $MYSQLOPT -D $DB < $SOURCE/db/$SCHEMA
  fi
done

cd ..
rm -rf .tmp
