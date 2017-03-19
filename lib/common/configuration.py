import os
import logging
from common.dataformat import Site

class Configuration(object):
    pass

logging.basicConfig(level = logging.INFO)

read_only = False

daemon_mode = False

num_threads = 32

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

dbs = Configuration()
dbs.url_base = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
#dbs.url_base = 'https://cmsweb.cern.ch/dbs/prod/phys03/DBSReader'

ssb = Configuration()
ssb.url_base = 'http://dashb-ssb.cern.ch/dashboard/request.py'

sitedb = Configuration()
sitedb.url_base = 'https://cmsweb.cern.ch/sitedb/data/prod'

popdb = Configuration()
popdb.url_base = 'https://cmsweb.cern.ch/popdb'

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
weblock.lock = 'https://cmst2.web.cern.ch/cmst2/unified/globallocks.json.lock'

mysqllock = Configuration()
mysqllock.db_params = {
    'config_file': '/etc/my.cnf',
    'config_group': 'mysql-dynamo',
    'db': 'dynamoregister'
}

tape_sites = ['T1_*_MSS', 'T0_CH_CERN_MSS']
disk_sites = ['T2_*', 'T1_*_Disk', 'T0_CH_CERN_Disk']

inventory = Configuration()
inventory.included_sites = disk_sites
inventory.excluded_sites = ['T2_CH_CERNBOX', 'T2_MY_UPM_BIRUNI', 'T1_US_FNAL_New_Disk']
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

demand = Configuration()
demand.access_history = Configuration()
demand.access_history.increment = 24 * 3600 # 24 hours
demand.access_history.max_back_query = 7 # maximum number of dates interval to obtain records for; 7 days
# give weight of bin[1] to now - bin[0]
demand.weight_time_bins = [
    (3600 * 24 * 7, 0.1),
    (3600 * 24 * 3, 0.5),
    (3600 * 24, 0.7),
    (3600 * 12, 1.)
]
