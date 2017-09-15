export DYNAMO_BASE=$(dirname $(dirname $(cd $(dirname ${BASH_SOURCE[0]}); pwd)))
export DYNAMO_ARCHIVE=/mnt/hadoop/$USER/dynamo
export DYNAMO_SPOOL=/var/spool/dynamo
export DYNAMO_DATADIR=/local/dynamo/dynamo
export DYNAMO_LOGDIR=/var/log/dynamo
export X509_USER_PROXY=/tmp/x509up_u$(id -u)

export PATH=$DYNAMO_BASE/bin:$(echo $PATH | sed "s|$DYNAMO_BASE/bin:||")
export PYTHONPATH=$DYNAMO_BASE/lib:$(echo $PYTHONPATH | sed "s|$DYNAMO_BASE/lib:||")
