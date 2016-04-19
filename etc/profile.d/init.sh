export DYNAMO_BASE=$(cd $(dirname $(dirname $(dirname ${BASH_SOURCE[0]}))); pwd)
export DYNAMO_LOGDIR=/var/log/dynamo
export PYTHONPATH=$DYNAMO_BASE/lib:$PYTHONPATH
export DETOX_CYCLE_HOURS=6
