#!/bin/bash

if [ $SERVER_DB_WRITE_CNF ] && [ $SERVER_DB_WRITE_CNFGROUP ]
then
  SUFFIX=$(echo $SERVER_DB_WRITE_CNFGROUP | sed 's/^mysql//')
  MYSQL="mysql -h localhost --defaults-file=$SERVER_DB_WRITE_CNF --defaults-group-suffix=$SUFFIX"
elif [ $SERVER_DB_WRITE_USER ] && [ $SERVER_DB_WRITE_PASSWD ]
then
  MYSQL="mysql -h localhost -u $SERVER_DB_WRITE_USER -p$SERVER_DB_WRITE_PASSWD"
fi

mkdir .tmp
cd .tmp

for SCHEMA in $(ls $SOURCE/db | grep '\.sql$')
do
  DB=$(echo $SCHEMA | sed 's/\.sql$//')
  $SOURCE/db/mysqldump.sh $DB

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
    echo "CREATE DATABASE $DB;" | $MYSQL
    $MYSQL -D $DB < $SOURCE/db/$SCHEMA
  fi
done

cd ..
rm -rf .tmp
