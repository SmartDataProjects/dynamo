import os
import logging

class Configuration(object):
    pass

logging.basicConfig(level = logging.INFO)

read_only = False

paths = Configuration()
paths.ddm_base = os.environ['DDM_BASE']
paths.log_directory = paths.ddm_base + '/logs'

webservice = Configuration()
#webservice.x509_key = '/tmp/x509up_u51268'
webservice.x509_key = '/tmp/x509up_u5410'

mysql = Configuration()
mysql.max_query_len = 900000 # allows up to 1M characters; allowing 10% safety margin

mysqlstore = Configuration()
mysqlstore.db = 'DDM_devel'
mysqlstore.host = 'localhost'
mysqlstore.user = 'ddmdevel'
mysqlstore.passwd = 'intelroccs'

phedex = Configuration()
phedex.url_base = 'https://cmsweb.cern.ch/phedex/datasvc/json/prod'

dbs = Configuration()
dbs.url_base = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
dbs.deletion_chunk_size = 40000000000000 # 40 TB

inventory = Configuration()
inventory.refresh_min = 21600 # 6 hours
inventory.included_sites = ['T2_*', 'T1_*_Disk']
inventory.included_groups = ['AnalysisOps', 'DataOps']
