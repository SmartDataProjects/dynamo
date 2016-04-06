import os
import logging

class Configuration(object):
    pass

logging.basicConfig(level = logging.INFO)

read_only = False

target_site_occupancy = 0.85

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

sitedb = Configuration()
sitedb.url_base = 'https://cmsweb.cern.ch/sitedb/data/prod'

popdb = Configuration()
popdb.url_base = 'https://cmsweb.cern.ch/popdb'

inventory = Configuration()
inventory.refresh_min = 21600 # 6 hours
inventory.included_sites = ['T2_*', 'T1_*_Disk']
inventory.excluded_sites = ['T2_CH_CERNBOX', 'T2_MY_UPM_BIRUNI']
inventory.included_groups = ['AnalysisOps', 'DataOps']

demand = Configuration()
demand.access_history = Configuration()
demand.access_history.increment = 24 * 3600 # 24 hours
demand.access_history.max_back_query = 7 # maximum number of dates interval to obtain records for; 7 days
