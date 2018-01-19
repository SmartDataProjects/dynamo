#!/bin/bash

[ $WEB_PATH ] || exit 1

rm -rf $WEB_PATH/html/dynamo
rm -rf $WEB_PATH/cgi-bin/dynamo
rm -rf $WEB_PATH/cgi-bin/registry
