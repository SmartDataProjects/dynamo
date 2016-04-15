import os
import logging

class Configuration(object):
    pass

logging.basicConfig(level = logging.INFO)

read_only = False

target_site_occupancy = 0.95

paths = Configuration()
paths.ddm_base = os.environ['DDM_BASE']
paths.log_directory = paths.ddm_base + '/logs'

history = Configuration()
history.db_params = {
    'host': 'localhost',
    'user': 'ddmdevel',
    'passwd': 'intelroccs',
    'db': 'history'
}

webservice = Configuration()
#webservice.x509_key = '/tmp/x509up_u51268'
webservice.x509_key = '/tmp/x509up_u5410'

mysql = Configuration()
mysql.max_query_len = 500000 # allows up to 1M characters; allowing 50% safety margin

mysqlstore = Configuration()
mysqlstore.db_params = {
    'host': 'localhost',
    'user': 'ddmdevel',
    'passwd': 'intelroccs',
    'db': 'DDM_devel'
}

phedex = Configuration()
phedex.url_base = 'https://cmsweb.cern.ch/phedex/datasvc/json/prod'
phedex.deletion_chunk_size = 40000000000000 # 40 TB
phedex.subscription_chunk_size = 40000000000000 # 40 TB

dbs = Configuration()
dbs.url_base = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'

sitedb = Configuration()
sitedb.url_base = 'https://cmsweb.cern.ch/sitedb/data/prod'

popdb = Configuration()
popdb.url_base = 'https://cmsweb.cern.ch/popdb'

globalqueue = Configuration()
globalqueue.collector = 'vocms099.cern.ch:9620'
globalqueue.schedd_constraint = 'CMSGWMS_Type =?= "crabschedd"'
globalqueue.job_constraint = 'TaskType=?="ROOT" && !isUndefined(DESIRED_CMSDataset)'

inventory = Configuration()
inventory.refresh_min = 21600 # 6 hours
inventory.included_sites = ['T2_*', 'T1_*_Disk']
inventory.excluded_sites = ['T2_CH_CERNBOX', 'T2_MY_UPM_BIRUNI', 'T1_US_FNAL_New_Disk']
inventory.included_groups = ['AnalysisOps', 'DataOps']

demand = Configuration()
demand.access_history = Configuration()
demand.access_history.increment = 24 * 3600 # 24 hours
demand.access_history.max_back_query = 7 # maximum number of dates interval to obtain records for; 7 days
