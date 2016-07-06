#!/bin/bash

TARGET=/var/www
HTMLTARGET=$TARGET/html
BINTARGET=$TARGET/cgi-bin

if [ -d $HTMLTARGET ] && [ -d $BINTARGET ]
then
  mkdir -p $HTMLTARGET/dynamo
  cp -r $DYNAMO_BASE/web/html/* $HTMLTARGET/dynamo/
  mkdir -p $BINTARGET/dynamo
  cp -r $DYNAMO_BASE/web/cgi-bin/* $BINTARGET/dynamo/
  [ -L $HTMLTARGET/dynamo/detox.php ] || ln -sf $BINTARGET/dynamo/detox/main.php $HTMLTARGET/dynamo/detox.php
  [ -L $HTMLTARGET/dynamo/inventory.php ] || ln -sf $BINTARGET/dynamo/inventory/main.php $HTMLTARGET/dynamo/inventory.php
fi
