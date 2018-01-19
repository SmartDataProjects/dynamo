#!/bin/bash

# mysqldump does not understand defaults file
#  --> Capture defaults file options here and convert them to user and password

OPTIONS=
while [ $# -gt 1 ]
do
  case $1 in
    --defaults-file*)
      if [[ $1 =~ --defaults-file=.* ]]
      then
        DEFAULTS_FILE=$(echo $1 | sed 's/[^=]*=\(.*\)/\1/')
        shift
      else
        DEFAULTS_FILE=$2
        shift 2
      fi
      ;;
    --defaults-group-suffix*)
      if [[ $1 =~ --defaults-group-suffix=.* ]]
      then
        DEFAULTS_SUFFIX=$(echo $1 | sed 's/[^=]*=\(.*\)/\1/')
        shift
      else
        DEFAULTS_SUFFIX=$2
        shift 2
      fi
      ;;
    *)
      OPTIONS=$OPTIONS" "$1
      shift
  esac
done

if [ $DEFAULTS_FILE ] || [ $DEFAULTS_SUFFIX ]
then
  [ $DEFAULTS_FILE ] || DEFAULTS_FILE=/etc/my.cnf

  INBLOCK=0
  while read LINE
  do
    if [ "$LINE" = "[mysql${DEFAULTS_SUFFIX}]" ]
    then
      INBLOCK=1
      continue
    fi
  
    [ $INBLOCK -eq 0 ] && continue
  
    KEY=$(echo $LINE | cut -d'=' -f 1)
    VALUE=$(echo $LINE | cut -d'=' -f 2)
  
    [ $KEY ] || break
  
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
  done < $DEFAULTS_FILE
fi

DB=$1

# Check DB exists
echo "SELECT 1;" | mysql $OPTIONS -D $DB >/dev/null 2>&1
if [ $? -ne 0 ]
then
  exit 1
fi

mysqldump $OPTIONS $DB -d $ARGS | sed -e 's/AUTO_INCREMENT=[0-9]*/AUTO_INCREMENT=1/' -e '/^\-\-/d' -e '/^\/\*/d' > $DB.sql
