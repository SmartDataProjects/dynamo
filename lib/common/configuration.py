import os
import logging

class Configuration(object):
    pass

logging.basicConfig(level = logging.INFO)

read_only = False

target_site_occupancy = 0.85

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
webservice.num_attempts = 5

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
    ('https://cmst2.web.cern.ch/cmst2/unified/globallocks.json', 'LIST_OF_DATASETS'),
    ('https://cmst2.web.cern.ch/cmst2/unified-testbed/globallocks.json', 'LIST_OF_DATASETS'),
    ('https://cmst1.web.cern.ch/CMST1/lockedData/lockTestSamples.json', 'SITE_TO_DATASETS'),
    ('https://cmsweb.cern.ch/t0wmadatasvc/prod/dataset_locked', 'CMSWEB_LIST_OF_DATASETS'),
    ('https://cmsweb.cern.ch/t0wmadatasvc/replayone/dataset_locked', 'CMSWEB_LIST_OF_DATASETS'),
    ('https://cmsweb.cern.ch/t0wmadatasvc/replaytwo/dataset_locked', 'CMSWEB_LIST_OF_DATASETS')
]
weblock.lock = 'https://cmst2.web.cern.ch/cmst2/unified/globallocks.json.lock'

inventory = Configuration()
inventory.refresh_min = 21600 # 6 hours
inventory.included_sites = ['T2_*', 'T1_*_Disk', 'T0_CH_CERN_Disk']
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


demand = Configuration()
demand.access_history = Configuration()
demand.access_history.increment = 24 * 3600 # 24 hours
demand.access_history.max_back_query = 7 # maximum number of dates interval to obtain records for; 7 days
demand.required_copies_def = [
    lambda d: 2 if d.name.endswith('MINIAOD') or d.name.endswith('MINIAODSIM') else -1,
    lambda d: 1 if d.name.endswith('AOD') or d.name.endswith('AODSIM') else -1
]
# give weight of bin[1] to now - bin[0]
demand.weight_time_bins = [
    (3600 * 24 * 7, 0.1),
    (3600 * 24 * 3, 0.5),
    (3600 * 24, 0.7),
    (3600 * 12, 1.)
]
