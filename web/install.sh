#!/bin/bash

echo "Installing web scripts."

if ! [ $SOURCE ] || ! [ $WEBPATH ]
then
  echo "Install source path is not set."
  exit 1
fi

TARGET=$WEBPATH

HTMLTARGET=$TARGET/html/dynamo
BINTARGET=$TARGET/cgi-bin
HTMLSOURCE=$SOURCE/web/html/dynamo
BINSOURCE=$SOURCE/web/cgi-bin

mkdir -p $HTMLTARGET
mkdir -p $BINTARGET

mkdir -p $HTMLTARGET/dynamo
cp -r $HTMLSOURCE/dynamo/* $HTMLTARGET/dynamo/
mkdir -p $HTMLTARGET/registry

mkdir -p $BINTARGET/dynamo
cp -r $BINSOURCE/dynamo/* $BINTARGET/dynamo/
mkdir -p $BINTARGET/registry
cp -r $BINSOURCE/registry/* $BINTARGET/registry/

[ -L $HTMLTARGET/dynamo/detox.php ] || ln -sf $BINTARGET/dynamo/detox/main.php $HTMLTARGET/dynamo/detox.php
[ -L $HTMLTARGET/dynamo/inventory.php ] || ln -sf $BINTARGET/dynamo/inventory/main.php $HTMLTARGET/dynamo/inventory.php
[ -L $HTMLTARGET/registry/detoxlock ] || ln -sf $BINTARGET/registry/detoxlock.php $HTMLTARGET/registry/detoxlock
[ -L $HTMLTARGET/registry/activitylock ] || ln -sf $BINTARGET/registry/activitylock.php $HTMLTARGET/registry/activitylock
[ -L $HTMLTARGET/registry/application ] || ln -sf $BINTARGET/registry/interface.php $HTMLTARGET/registry/application
