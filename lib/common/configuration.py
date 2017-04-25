import os
import re
import time
import logging
from common.dataformat import Site

class Configuration(object):
    pass

logging.basicConfig(level = logging.INFO)

read_only = False

daemon_mode = False

use_threads = True
num_threads = 32 # default value for parallel_exec; actual number of threads depends on the function caller
multi_thread_repeat_exception = True # when a thread hits an exception, repeat the thread executable for full debugging

show_time_profile = True

paths = Configuration()
paths.base = os.environ['DYNAMO_BASE']
paths.data = os.environ['DYNAMO_DATADIR']

mysqlhistory = Configuration()
mysqlhistory.db_params = {
    'config_file': '/etc/my.cnf',
    'config_group': 'mysql-dynamo',
    'db': 'dynamohistory'
}

webservice = Configuration()
webservice.x509_key = os.environ['X509_USER_PROXY']
webservice.cookie_file = os.environ['DYNAMO_DATADIR'] + '/cookies.txt'
webservice.num_attempts = 20
webservice.cache_db_params = {
    'config_file': '/etc/my.cnf',
    'config_group': 'mysql-dynamo',
    'host': 't3serv003.mit.edu',
    'db': 'dynamocache'
}

mysql = Configuration()
mysql.max_query_len = 100000 # allows up to 1M characters; allowing 90% safety margin

mysqlstore = Configuration()
mysqlstore.db_params = {
    'config_file': '/etc/my.cnf',
    'config_group': 'mysql-dynamo',
    'db': 'dynamo'
}

phedex = Configuration()
phedex.url_base = 'https://cmsweb.cern.ch/phedex/datasvc/json/prod'
phedex.subscription_chunk_size = 4.e+13 # 40 TB
phedex.cache_lifetime = 21600 # cache lifetime in seconds (6 hours)

dbs = Configuration()
dbs.url_base = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
#dbs.url_base = 'https://cmsweb.cern.ch/dbs/prod/phys03/DBSReader'

ssb = Configuration()
ssb.url_base = 'http://dashb-ssb.cern.ch/dashboard/request.py'

sitedb = Configuration()
sitedb.url_base = 'https://cmsweb.cern.ch/sitedb/data/prod'

popdb = Configuration()
popdb.url_base = 'https://cmsweb.cern.ch/popdb'
popdb.max_back_query = 7 # maximum number of dates interval to obtain records for; 7 days

jobqueue = Configuration()
jobqueue.weight_halflife = 3. # half life of job queue weight in days

globalqueue = Configuration()
globalqueue.collector = 'cmsgwms-collector-global.cern.ch:9620'

weblock = Configuration()
weblock.sources = [
    ('https://vocms049.cern.ch/unified/public/globallocks.json', 'noauth', 'LIST_OF_DATASETS'),
    ('https://cmst2.web.cern.ch/cmst2/unified-testbed/globallocks.json', 'cert', 'LIST_OF_DATASETS'),
    ('https://cmst1.web.cern.ch/CMST1/lockedData/lockTestSamples.json', 'cert', 'SITE_TO_DATASETS'),
    ('https://cmsweb.cern.ch/t0wmadatasvc/prod/dataset_locked', 'cert', 'CMSWEB_LIST_OF_DATASETS'),
    ('https://cmsweb.cern.ch/t0wmadatasvc/replayone/dataset_locked', 'cert', 'CMSWEB_LIST_OF_DATASETS'),
    ('https://cmsweb.cern.ch/t0wmadatasvc/replaytwo/dataset_locked', 'cert', 'CMSWEB_LIST_OF_DATASETS')
]
weblock.lock = 'https://vocms049.cern.ch/unified/public/globallocks.json.lock'

mysqllock = Configuration()
mysqllock.db_params = {
    'config_file': '/etc/my.cnf',
    'config_group': 'mysql-dynamo',
    'db': 'dynamoregister'
}
mysqllock.users = [('%%', '%%')] # list of (user, service) 2-tuples

tape_sites = ['T1_*_MSS', 'T0_CH_CERN_MSS']
disk_sites = ['T2_*', 'T1_*_Disk', 'T0_CH_CERN_Disk']

inventory = Configuration()
inventory.included_sites = disk_sites
inventory.excluded_sites = [
    'T1_US_FNAL_New_Disk', # not a valid site
    'T2_CH_CERNBOX', # not a valid site
    'T2_MY_UPM_BIRUNI', # inheriting from IntelROCCS status 0
    'T2_PK_NCP', # inheriting from IntelROCCS status 0
    'T2_PL_Warsaw', # inheriting from IntelROCCS status 0
    'T2_RU_ITEP', # inheriting from IntelROCCS status 0
    'T2_RU_PNPI', # inheriting from IntelROCCS status 0
    'T2_TH_CUNSTDA' # inheriting from IntelROCCS status 0
]
inventory.included_groups = [
    'AnalysisOps', 'DataOps', 'FacOps', 'IB RelVal', 'RelVal',
    'B2G',
    'SMP',
    'b-physics',
    'b-tagging',
    'caf-alca',
    'caf-comm',
    'caf-lumi',
    'caf-phys',
#    'deprecated-ewk',
#    'deprecated-qcd',
#    'deprecated-undefined',
    'dqm',
    'e-gamma_ecal',
    'exotica',
    'express',
    'forward',
    'heavy-ions',
    'higgs',
    'jets-met_hcal',
    'local',
    'muon',
    'susy',
    'tau-pflow',
    'top',
    'tracker-dpg',
    'tracker-pog',
    'trigger',
    'upgrade'
]
# list of (partition name, partitioning function)
inventory.partitions = [
    ('AnalysisOps', lambda r: r.group is not None and r.group.name == 'AnalysisOps'),
    ('DataOps', lambda r: r.group is not None and r.group.name == 'DataOps'),
    ('RelVal', lambda r: r.group is not None and r.group.name == 'RelVal'),
    ('caf-comm', lambda r: r.group is not None and r.group.name == 'caf-comm'),
    ('caf-alca', lambda r: r.group is not None and r.group.name == 'caf-alca'),
    ('local', lambda r: r.group is not None and r.group.name == 'local'),
    ('IB RelVal', lambda r: r.group is not None and r.group.name == 'IB RelVal'),
    ('Tape', lambda r: r.site.storage_type == Site.TYPE_MSS),
    ('Unsubscribed', lambda r: r.group is None),
    ('Physics', lambda r: r.group is not None and (r.group.name == 'AnalysisOps' or r.group.name == 'DataOps'))
]
# list of conditions for a PRODUCTION state dataset to become IGNORED (will still be reset to PRODUCTION if a new block replica is found)
inventory.ignore_datasets = [
    lambda d: (time.time() - d.last_update) / 3600. / 24. > 180,
    lambda d: re.match('.*test.*', d.name, re.IGNORECASE),
    lambda d: re.match('.*BUNNIES.*', d.name),
    lambda d: re.match('.*/None.*', d.name),
    lambda d: re.match('.*FAKE.*', d.name)
]
