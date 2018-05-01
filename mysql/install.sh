#!/bin/bash

###########################################################################################
## install.sh
##
## Sets up MySQL databases. Can be run even after the DB is set up (will not overwrite).
## Compares the schema in the repository with the existing; if differences are found,
## prints a warning but does not do anything.
###########################################################################################

THISDIR=$(cd $(dirname $0); pwd)

source $THISDIR/../utilities/shellutils.sh

echo '##################################'
echo '######  MYSQL DEPENDENCIES  ######'
echo '##################################'
echo

echo "-> Checking dependencies.."

# Need the server running
require rpm -q MySQL-python
require pgrep -f mysqld

ROOTCNF=/etc/my.cnf.d/root.cnf
HAS_ROOTCNF=true

# If ROOTCNF does not exist, make a temporary file
if [ -r $ROOTCNF ]
then
  MYSQLOPT="--defaults-file=$ROOTCNF"
else
  echo -n 'Enter password for MySQL root:'
  read -s PASSWD
  echo

  MYSQLOPT="-u root -p$PASSWD -h localhost"

  unset PASSWD

  HAS_ROOTCNF=false
fi

MYSQL="mysql -A $MYSQLOPT"

# Set up grants
echo '############################'
echo '######  MYSQL GRANTS  ######'
echo '############################'
echo

python $THISDIR/grants.py -q $MYSQLOPT

# Set up databases
echo '##########################################'
echo '######  MYSQL DATABASES AND TABLES  ######'
echo '##########################################'
echo

HAS_DIFFERENCE=false

for DB in $(ls $THISDIR/schema) dynamo_tmp
do
  echo "$DB .."

  # Does the database exist?
  echo 'SELECT 1;' | $MYSQL -D $DB > /dev/null 2>&1

  # If not, create it
  if [ $? -ne 0 ]
  then
    echo 'CREATE DATABASE `'$DB'`;'
    echo 'CREATE DATABASE `'$DB'`;' | $MYSQL > /dev/null 2>&1
    if [ $? -ne 0 ]
    then
      echo "Database creation failed."
      exit 1
    fi
  fi

  [ -d $THISDIR/schema/$DB ] || continue

  for SQL in $(ls $THISDIR/schema/$DB)
  do
    TABLE=$(echo $SQL | sed 's/.sql//')

    # Get the CREATE TABLE command. If the table does not exist, return code is nonzero
    python $THISDIR/get_schema.py $MYSQLOPT $DB $TABLE > /tmp/.schema.$$ 2>&1

    if [ $? -ne 0 ]
    then
      echo 'Creating new table '$DB'.'$TABLE
      $MYSQL -D $DB < $THISDIR/schema/$DB/$SQL
      echo
    elif ! diff /tmp/.schema.$$ $THISDIR/schema/$DB/$SQL > /dev/null
    then
      echo 'Difference found in '$DB'.'$TABLE' (existing | installation):'
      echo
      diff -y /tmp/.schema.$$ $THISDIR/schema/$DB/$SQL
      echo
      HAS_DIFFERENCE=true
    fi

    rm /tmp/.schema.$$
  done
done

if $HAS_ROOTCNF
then
  echo "Set up DB backup cron job [y/n]?"
  if confirmed
  then
    READCONF="$SOURCE/utilities/readconf -I $SOURCE/dynamo.cfg"
    INSTALL_PATH=$($READCONF paths.dynamo_base)

    crontab -l -u root > /tmp/crontab.tmp.$$
    chmod 600 /tmp/crontab.tmp.$$
    if ! grep -q $INSTALL_PATH/sbin/backup /tmp/crontab.tmp.$$
    then
      echo "Setting up cron job"
      echo "  00 01 * * * $INSTALL_PATH/sbin/mysql_backup > $LOG_PATH/backup.log 2>&1"
      echo
      echo "Note: It is advised to set up binary logging of dynamohistory and dynamoregister by adding"
      echo "the following lines to the [mysqld] section of /etc/my.cnf:"
      echo "  log-bin=/var/log/mysql/mysqld.log"
      echo "  binlog-do-db=dynamohistory"
      echo "  binlog-do-db=dynamoregister"
      echo
  
      echo "00 01 * * * $INSTALL_PATH/sbin/mysql_backup > $LOG_PATH/backup.log 2>&1" >> /tmp/crontab.tmp.$$
      crontab -u root - < /tmp/crontab.tmp.$$
    fi
    rm /tmp/crontab.tmp.$$
  fi
else
  echo "MySQL root credentials were not found in $ROOTCNF."
  echo "No automatic DB backup is set up."
  echo
fi

echo "MySQL installation is complete."
$HAS_DIFFERENCE && echo "Plase fix the schema differences before starting Dynamo."
echo

exit 0
