export DDM_BASE=$(cd $(dirname $(dirname $(dirname ${BASH_SOURCE[0]}))); pwd)
export PYTHONPATH=$DDM_BASE/lib:$PYTHONPATH
