#!/bin/bash

###########################################################################################
## uninstall.sh
##
## Drop all tables and databases
###########################################################################################

THISDIR=$(cd $(dirname $0); pwd)

source $THISDIR/../utilities/shellutils.sh

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

MYSQL="mysql $MYSQLOPT"

# Set up databases
echo '##########################################'
echo '######  MYSQL DATABASES AND TABLES  ######'
echo '##########################################'
echo

for DB in $(ls $THISDIR/schema) dynamo_tmp
do
  echo "$DB .."

  echo 'DROP DATABASE `'$DB'`;'
  echo 'DROP DATABASE `'$DB'`;' | $MYSQL > /dev/null 2>&1
done

# Set up grants
echo '############################'
echo '######  MYSQL GRANTS  ######'
echo '############################'
echo

python $THISDIR/grants.py -q $MYSQLOPT --revoke

echo "MySQL uninstallation is complete."
echo

exit 0
