export DYNAMO_BASE=$(dirname $(dirname $(cd $(dirname ${BASH_SOURCE[0]}); pwd)))
export DYNAMO_LOGDIR=/var/log/dynamo
export DYNAMO_DETOX_INTERVAL=6
export DYNAMO_DEALER_INTERVAL=1
export X509_USER_PROXY=/tmp/x509up_u$(id -u)
export PYTHONPATH=$DYNAMO_BASE/lib:$(echo $PYTHONPATH | sed "s|$DYNAMO_BASE/lib:||")
