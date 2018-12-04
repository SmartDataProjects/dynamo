#! /bin/bash

# Check that the server works
su -c "printf '' | dynamo 2>&1 | grep 'DYNAMO'" dynamo
