#!/bin/bash

###########################################################################################
## grants.sh
##
## Run as root. Sets up MySQL users and grants for the MySQL accounts necessary for Dynamo
## to operate. Optionally creates MySQL defaults file under /etc/my.cnf.d/.
###########################################################################################

echo
echo "Setting up MySQL accounts."
echo

confirmed () {
  while true
  do
    read RESPONSE
    case $RESPONSE in
      y)
        return 0
        ;;
      n)
        return 1
        ;;
      *)
        echo "Please answer in y/n."
        ;;
    esac
  done
}

require () {
  "$@" >/dev/null 2>&1 && return 0
  echo
  echo "[Fatal] Failed: $@"
  exit 1
}

checkmysql () {
  TEST=$(echo "SELECT 1;" | mysql -h localhost --skip-column-names "$@" 2>&1)
  if [ "$TEST" = "1" ] || [[ $TEST =~ "ERROR 1045" ]]  # 1045: Access denied for user
  then
    return 0
  else
    return 1
  fi
}

warnifnot () {
  "$@" >/dev/null 2>&1 && return 0
  echo
  echo "[Warning] Failed: $@"
  echo "Some components may not work."
}

## Test if MySQL is running on localhost
require which mysql
if ! checkmysql
then
  echo
  echo "[Fatal] MySQL not running on localhost"
  exit 1
fi

## Ask for the root password
echo "root password:"
read ROOT_PASSWD

if ! checkmysql -u root -p"$ROOT_PASSWD"
then
  echo
  echo "Wrong password."
fi

ROOTSQL="mysql -u root -p"$ROOT_PASSWD" -h localhost"

new_user () {
  USER=$1
  HOST=$2
  PASSWD=$3

  NEXIST=$(echo 'SELECT COUNT(*) FROM `mysql`.`user` WHERE `User` = "'$USER'" AND `Host` = "'$HOST'";' | $ROOTSQL --skip-column-names)
  if [ $NEXIST -gt 0 ]
  then
    echo
    echo "User $USER already exists."
    echo "SET PASSWORD FOR '$USER'@'$HOST' = PASSWORD('$PASSWD');" | $ROOTSQL
  else
    echo "CREATE USER '$USER'@'$HOST' IDENTIFIED BY '$PASSWD';" | $ROOTSQL
  fi
}


## Ask for user names
echo 'User name for the Dynamo server (config.sh:$SERVER_DB_WRITE_USER):'
read SERVER_USER
echo "Password for $SERVER_USER:"
read SERVER_USER_PASSWD
echo 'User name for elevated-privilege executables (write-allowed to some tables; not in config.sh):'
read PRIV_USER
echo "Password for $PRIV_USER:"
read PRIV_USER_PASSWD
echo 'User name for normal executables (config.sh:$SERVER_DB_READ_USER):'
read NORMAL_USER
echo "Password for $NORMAL_USER:"
read NORMAL_USER_PASSWD

## Create users and add grants
for HOST in 'localhost' '%'
do
  new_user $SERVER_USER $HOST $SERVER_USER_PASSWD
  new_user $PRIV_USER $HOST $PRIV_USER_PASSWD
  new_user $NORMAL_USER $HOST $NORMAL_USER_PASSWD

  # SERVER_USER gets all privileges, so wildcard is fine
  echo 'GRANT ALL PRIVILEGES ON `dynamo%`.* TO "'$SERVER_USER'"@"'$HOST'";' | $ROOTSQL

  # Other users have table-specific grants, in which case use of wildcard can result in unexpected behavior

  # DB-wide operations (PRIV_USER)
  echo 'GRANT SELECT ON `dynamo`.* TO "'$PRIV_USER'"@"'$HOST'";' | $ROOTSQL
  echo 'GRANT ALL PRIVILEGES ON `dynamo\_tmp`.* TO "'$PRIV_USER'"@"'$HOST'";' | $ROOTSQL
  echo 'GRANT SELECT, LOCK TABLES ON `dynamoregister`.* TO "'$PRIV_USER'"@"'$HOST'";' | $ROOTSQL
  echo 'GRANT SELECT, INSERT, UPDATE, DELETE, LOCK TABLES ON `dynamohistory`.* TO "'$PRIV_USER'"@"'$HOST'";' | $ROOTSQL
  echo 'GRANT SELECT, INSERT, UPDATE, DELETE, LOCK TABLES, CREATE, DROP ON `dynamohistory\_cache`.* TO "'$PRIV_USER'"@"'$HOST'";' | $ROOTSQL

  # Table-specific operations (PRIV_USER)
  echo 'GRANT UPDATE ON `dynamo`.`system` TO "'$PRIV_USER'"@"'$HOST'";' | $ROOTSQL
  echo 'GRANT INSERT, UPDATE, DELETE ON `dynamo`.`dataset_requests` TO "'$PRIV_USER'"@"'$HOST'";' | $ROOTSQL
  echo 'GRANT INSERT, UPDATE, DELETE ON `dynamo`.`dataset_accesses` TO "'$PRIV_USER'"@"'$HOST'";' | $ROOTSQL
  echo 'GRANT INSERT, UPDATE, DELETE ON `dynamoregister`.`activity_lock` TO "'$PRIV_USER'"@"'$HOST'";' | $ROOTSQL
  echo 'GRANT INSERT, UPDATE, DELETE ON `dynamoregister`.`copy_requests` TO "'$PRIV_USER'"@"'$HOST'";' | $ROOTSQL
  echo 'GRANT INSERT, UPDATE, DELETE ON `dynamoregister`.`copy_request_items` TO "'$PRIV_USER'"@"'$HOST'";' | $ROOTSQL
  echo 'GRANT INSERT, UPDATE, DELETE ON `dynamoregister`.`active_copies` TO "'$PRIV_USER'"@"'$HOST'";' | $ROOTSQL
  echo 'GRANT INSERT, UPDATE, DELETE ON `dynamoregister`.`deletion_requests` TO "'$PRIV_USER'"@"'$HOST'";' | $ROOTSQL
  echo 'GRANT INSERT, UPDATE, DELETE ON `dynamoregister`.`deletion_request_items` TO "'$PRIV_USER'"@"'$HOST'";' | $ROOTSQL
  echo 'GRANT INSERT, UPDATE, DELETE ON `dynamoregister`.`active_deletions` TO "'$PRIV_USER'"@"'$HOST'";' | $ROOTSQL
  echo 'GRANT INSERT, UPDATE, DELETE ON `dynamoregister`.`deletion_queue` TO "'$PRIV_USER'"@"'$HOST'";' | $ROOTSQL
  echo 'GRANT INSERT, UPDATE, DELETE ON `dynamoregister`.`transfer_queue` TO "'$PRIV_USER'"@"'$HOST'";' | $ROOTSQL
  echo 'GRANT INSERT, UPDATE, DELETE ON `dynamoregister`.`stage_queue` TO "'$PRIV_USER'"@"'$HOST'";' | $ROOTSQL

  # DB-wide operations (NORMAL_USER)
  echo 'GRANT SELECT ON `dynamo`.* TO "'$NORMAL_USER'"@"'$HOST'";' | $ROOTSQL
  echo 'GRANT ALL PRIVILEGES ON `dynamo\_tmp`.* TO "'$NORMAL_USER'"@"'$HOST'";' | $ROOTSQL
  echo 'GRANT SELECT ON `dynamoregister`.* TO "'$NORMAL_USER'"@"'$HOST'";' | $ROOTSQL
  echo 'GRANT SELECT, LOCK TABLES ON `dynamohistory`.* TO "'$NORMAL_USER'"@"'$HOST'";' | $ROOTSQL
  echo 'GRANT SELECT, INSERT, UPDATE, DELETE, LOCK TABLES, CREATE, DROP ON `dynamohistory\_cache`.* TO "'$PRIV_USER'"@"'$HOST'";' | $ROOTSQL

  # Table-specific operations (NORMAL_USER)
  echo 'GRANT UPDATE ON `dynamohistory`.`lock` TO "'$NORMAL_USER'"@"'$HOST'";' | $ROOTSQL
  echo 'GRANT INSERT, UPDATE, DELETE ON `dynamoregister`.`activity_lock` TO "'$NORMAL_USER'"@"'$HOST'";' | $ROOTSQL
done

## Write my.cnf files (optional)
echo "Write my.cnf files? [y/n]"
confirmed || exit

echo "UNIX user name for elevated-privilege executables:"
read UNIX_PRIV_USER

mkdir -p /etc/my.cnf.d

echo '[mysql]
host=localhost
user='$SERVER_USER'
password='$SERVER_USER_PASSWD > /etc/my.cnf.d/dynamo-write.cnf
chmod 600 /etc/my.cnf.d/dynamo-write.cnf

echo '[mysql]
host=localhost
user='$PRIV_USER'
password='$PRIV_USER_PASSWD > /etc/my.cnf.d/dynamo.cnf
chown $UNIX_PRIV_USER:$(id -gn $UNIX_PRIV_USER) /etc/my.cnf.d/dynamo.cnf
chmod 640 /etc/my.cnf.d/dynamo.cnf

echo '[mysql]
host=localhost
user='$SERVER_USER'
password='$SERVER_USER_PASSWD > /etc/my.cnf.d/dynamo-read.cnf
