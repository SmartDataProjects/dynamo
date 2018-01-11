#!/bin/bash

[ $WEBPATH ] || exit 1

rm -rf $WEBPATH/html/dynamo
rm -rf $WEBPATH/cgi-bin/dynamo
rm -rf $WEBPATH/cgi-bin/registry