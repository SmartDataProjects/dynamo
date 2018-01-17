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
  TEST=$(echo "SELECT 1;" | mysql -h localhost --skip-column-names "$@")
  return [ $TEST = "1" ] || [[ $TEST =~ "ERROR 1045" ]]  # 1045: Access denied for user
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

ROOTSQL=$(mysql -u root -p"$ROOT_PASSWD")

## Ask for user names
echo "User name for the Dynamo server (config.sh:$SERVER_DB_WRITE_USER):"
read SERVER_USER
echo "Password for $SERVER_USER:"
read SERVER_USER_PASSWD
echo "User name for elevated-privilege executables (write-allowed to some tables; not in config.sh):"
read PRIV_USER
echo "Password for $PRIV_USER:"
read PRIV_USER_PASSWD
echo "User name for normal executables (config.sh:$SERVER_DB_READ_USER):"
read NORMAL_USER
echo "Password for $NORMAL_USER:"
read NORMAL_USER_PASSWD

for USER in $SERVER_USER $PRIV_USER $NORMAL_USER
do
  NEXIST=$(echo 'SELECT COUNT(*) FROM `mysql`.`user` WHERE User = "'$USER'";' | $ROOTSQL --skip-column-names)
  if [ $NEXIST -gt 0 ]
  then
    echo
    echo "User $USER already exists."
    exit 1
  fi
done

## Create users and add grants
for HOST in 'localhost' '%'
do
  echo "CREATE USER '$SERVER_USER'@'$HOST' IDENTIFIED BY '$SERVER_USER_PASSWD';" | $ROOTSQL
  echo 'GRANT ALL PRIVILEGES ON `dynamo%`.* TO "'$SERVER_USER'"@"'$HOST'";' | $ROOTSQL

  echo "CREATE USER '$PRIV_USER'@'$HOST' IDENTIFIED BY '$PRIV_USER_PASSWD';" | $ROOTSQL
  echo "GRANT ALL PRIVILEGES ON `dynamo_tmp`.* TO '$PRIV_USER'@'$HOST';" | $ROOTSQL
  echo 'GRANT SELECT ON `dynamo%`.* TO "'$PRIV_USER'"@"'$HOST'";' | $ROOTSQL
  echo 'GRANT UPDATE ON `dynamo`.`system` TO "'$PRIV_USER'"@"'$HOST'";' | $ROOTSQL
  echo 'GRANT SELECT, INSERT, UPDATE, DELETE, DROP ON `dynamohistory`.* TO "'$PRIV_USER'"@"'$HOST'";' | $ROOTSQL
  echo 'GRANT SELECT, INSERT, UPDATE, DELETE ON `dynamo`.`dataset_requests` TO "'$PRIV_USER'"@"'$HOST'";' | $ROOTSQL
  echo 'GRANT SELECT, INSERT, UPDATE, DELETE ON `dynamo`.`dataset_accesses` TO "'$PRIV_USER'"@"'$HOST'";' | $ROOTSQL

  echo "CREATE USER '$NORMAL_USER'@'$HOST' IDENTIFIED BY '$NORMAL_USER_PASSWD';" | $ROOTSQL
  echo 'GRANT ALL PRIVILEGES ON `dynamo_tmp`.* TO "'$NORMAL_USER'"@"'$HOST'";' | $ROOTSQL
  echo 'GRANT SELECT ON `dynamo%`.* TO "'$NORMAL_USER'"@"'$HOST'";' | $ROOTSQL
done

## Write my.cnf files (optional)
echo "Write my.cnf files? [y/n]"
if confirmed
then
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

fi
