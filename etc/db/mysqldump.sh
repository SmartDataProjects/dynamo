#!/bin/bash

ARGS="$@" # pass -h, -u, and -p options, and the DB name at the end
DB="${@: -1}"

mysqldump -d $ARGS | sed 's/AUTO_INCREMENT=[0-9]*/AUTO_INCREMENT=1/' > $DB.sql
