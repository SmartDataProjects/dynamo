#!/bin/bash
# Dynamo check for Nagios
# Version 1.0, last modified: 24.03.2017
# Bugs and comments to Yutaro Iiyama (yiiyama@mit.edu)
#
# ChangeLog

VERSION=1.0

STATUS_OK=0          
STATUS_WARNING=1     # Not used
STATUS_CRITICAL=2    # Process stopped or DB backup old
STATUS_UNKNOWN=3     # Internal or usage error

BRIEF_INFO="OK"
RETURN_STATUS=$STATUS_OK


usage() {
   /bin/echo "Usage:   $0"
   /bin/echo "Checks dynamo processes and DB backups."
}

version() {
   /bin/echo "Dynamo check for Nagios, version: $VERSION"
}

help() {
   version
   usage
}

# Appends information to the brief output string
append_info() {
   local info; info=$1
   
   if [ "x$BRIEF_INFO" == "xOK" ]; then
      BRIEF_INFO=$info
   else
      BRIEF_INFO="${BRIEF_INFO}; $info"
   fi
}

# Option handling
while getopts "hVv" opt; do
  case $opt in
    h)
      help
      exit $STATUS_OK
    ;;
    V)
      version
      exit $STATUS_OK
    ;;  
    v)
      /bin/echo "verbose mode"
    ;;  
    *)
      /bin/echo "SERVICE STATUS: Invalid option: $1"
      exit $STATUS_UNKNOWN 
      ;;
  esac
done
shift $[$OPTIND-1]

# Check process
PID=$(/usr/bin/pgrep -f dynamod)
if [ $PID ]
then
  append_info "dynamod process id $PID"
else
  append_info "dynamod process not running"
  RETURN_STATUS=$STATUS_CRITICAL
fi

# Check database backups
BACKUP=$(/bin/find _ARCHIVEPATH_/db -name dynamo_*.gz -mtime -1)
if ! [ $BACKUP ]
then
  append_info "inventory DB backup younger than 24 hours does not exist"
  RETURN_STATUS=$STATUS_CRITICAL
fi

BACKUP=$(/bin/find _ARCHIVEPATH_/db -name dynamohistory_*.gz -mtime -1)
if ! [ $BACKUP ]
then
  append_info "history DB backup younger than 24 hours does not exist"
  RETURN_STATUS=$STATUS_CRITICAL
fi

/bin/echo "SERVICE STATUS: $BRIEF_INFO"
exit $RETURN_STATUS
