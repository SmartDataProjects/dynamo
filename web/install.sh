#!/bin/bash

###########################################################################################
## install.sh
##
## Sets up HTML templates and contents.
###########################################################################################

THISDIR=$(cd $(dirname $0); pwd)

source $THISDIR/../utilities/shellutils.sh

echo '#############################'
echo '######  HTML TEMPLATES ######'
echo '#############################'
echo

READCONF="$THISDIR/../utilities/readconf -I $THISDIR/../dynamo.cfg"

CONTENTS_PATH=$($READCONF web.contents_path)
mkdir -p $CONTENTS_PATH

cp -r $THISDIR/html $CONTENTS_PATH/html
