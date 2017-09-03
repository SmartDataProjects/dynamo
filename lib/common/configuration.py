import os
import socket
import re
import time
import logging
from common.dataformat import Site

class Configuration(object):
    def __init__(self, **kwd):
        for key, value in kwd.items():
            setattr(self, key, value)

    def isset(self, name):
        return hasattr(self, name)


logging.basicConfig(level = logging.INFO)

hostname = socket.gethostname()
ipaddr = socket.getaddrinfo(hostname, 0)[0][4][0]
if ipaddr == socket.getaddrinfo('dynamo.mit.edu', 0)[0][4][0]:
    # this is the production server
    hostname = 'dynamo.mit.edu'

read_only = False

daemon_mode = False

use_threads = True
num_threads = 32 # default value for parallel_exec; actual number of threads depends on the function caller
multi_thread_repeat_exception = True # when a thread hits an exception, repeat the thread executable for full debugging

show_time_profile = True

paths = Configuration(
    base = os.environ['DYNAMO_BASE'],
    data = os.environ['DYNAMO_DATADIR']
)

mysqlhistory = Configuration(
    db_params = {
        'config_file': '/etc/my.cnf',
        'config_group': 'mysql-dynamo',
        'db': 'dynamohistory'
    }
)

webservice = Configuration(
    x509_key = os.environ['X509_USER_PROXY'],
    cookie_file = os.environ['DYNAMO_DATADIR'] + '/cookies.txt',
    num_attempts = 20,
    cache_db_params = {
        'config_file': '/etc/my.cnf',
        'config_group': 'mysql-dynamo',
        'host': 't3serv003.mit.edu',
        'db': 'dynamocache'
    }
)

mysql = Configuration(
    max_query_len = 100000 # allows up to 1M characters; allowing 90% safety margin
)

mysqlstore = Configuration(
    db_params = {
        'config_file': '/etc/my.cnf',
        'config_group': 'mysql-dynamo',
        'db': 'dynamo'
    }
)

phedex = Configuration(
    url_base = 'https://cmsweb.cern.ch/phedex/datasvc/json/prod',
    subscription_chunk_size = 4.e+13, # 40 TB
    cache_lifetime = 0 # cache lifetime in seconds (6 hours)
)

dbs = Configuration(
    url_base = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
    #dbs.url_base = 'https://cmsweb.cern.ch/dbs/prod/phys03/DBSReader'
)

ssb = Configuration(
    url_base = 'http://dashb-ssb.cern.ch/dashboard/request.py'
)

sitedb = Configuration(
    url_base = 'https://cmsweb.cern.ch/sitedb/data/prod'
)

popdb = Configuration(
    url_base = 'https://cmsweb.cern.ch/popdb',
    max_back_query = 7 # maximum number of dates interval to obtain records for; 7 days
)

jobqueue = Configuration(
    weight_halflife = 3. # half life of job queue weight in days
)

globalqueue = Configuration(
    collector = 'cmsgwms-collector-global.cern.ch:9620'
)

weblock = Configuration(
    sources = [
        ('https://vocms049.cern.ch/unified/public/globallocks.json', 'noauth', 'LIST_OF_DATASETS', 'T*'),
        ('https://cmst2.web.cern.ch/cmst2/unified-testbed/globallocks.json', 'cert', 'LIST_OF_DATASETS', 'T*'),
        ('https://cmst1.web.cern.ch/CMST1/lockedData/lockTestSamples.json', 'cert', 'SITE_TO_DATASETS', 'T*'),
        ('https://cmsweb.cern.ch/t0wmadatasvc/prod/dataset_locked', 'cert', 'CMSWEB_LIST_OF_DATASETS', 'T0_CH_CERN_Disk'),
        ('https://cmsweb.cern.ch/t0wmadatasvc/replayone/dataset_locked', 'cert', 'CMSWEB_LIST_OF_DATASETS', 'T0_CH_CERN_Disk'),
        ('https://cmsweb.cern.ch/t0wmadatasvc/replaytwo/dataset_locked', 'cert', 'CMSWEB_LIST_OF_DATASETS', 'T0_CH_CERN_Disk')
    ],
    lock = 'https://vocms049.cern.ch/unified/public/globallocks.json.lock'
)

mysqllock = Configuration(
    users = [('%%', '%%')] # list of (user, service) 2-tuples
)

activitylock = Configuration(
    default_user = 'paus'
)

tape_sites = ['T1_*_MSS', 'T0_CH_CERN_MSS']
disk_sites = ['T2_*', 'T1_*_Disk', 'T0_CH_CERN_Disk']

inventory = Configuration(
    included_sites = disk_sites,
    excluded_sites = [
        'T1_US_FNAL_New_Disk', # not a valid site
        'T2_CH_CERNBOX', # not a valid site
        'T2_MY_UPM_BIRUNI', # site not in popDB
        'T2_KR_KISTI' # site not in popDB
#        'T2_PK_NCP', # inheriting from IntelROCCS status 0
#        'T2_PL_Warsaw', # inheriting from IntelROCCS status 0
#        'T2_RU_ITEP', # inheriting from IntelROCCS status 0
#        'T2_RU_PNPI', # inheriting from IntelROCCS status 0
#        'T2_TH_CUNSTDA' # inheriting from IntelROCCS status 0
    ],
    included_groups = [
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
    ],
    # list of (partition name, partitioning function)
    partitions = [
        ('AnalysisOps', lambda r: r.group is not None and r.group.name == 'AnalysisOps'),
        ('DataOps', lambda r: r.group is not None and r.group.name == 'DataOps'),
        ('RelVal', lambda r: r.group is not None and r.group.name == 'RelVal'),
        ('caf-comm', lambda r: r.group is not None and r.group.name == 'caf-comm'),
        ('caf-alca', lambda r: r.group is not None and r.group.name == 'caf-alca'),
        ('local', lambda r: r.group is not None and r.group.name == 'local'),
        ('IB RelVal', lambda r: r.group is not None and r.group.name == 'IB RelVal'),
        ('Tape', lambda r: r.site.storage_type == Site.TYPE_MSS),
        ('Unsubscribed', lambda r: r.group is None),
        ('Physics', ['AnalysisOps', 'DataOps'])
    ],
    # list of conditions for a PRODUCTION state dataset to become IGNORED (will still be reset to PRODUCTION if a new block replica is found)
    ignore_datasets = [
        lambda d: (time.time() - d.last_update) / 3600. / 24. > 180,
        lambda d: re.match('/.+test.*', d.name, re.IGNORECASE), # don't match to TestEnables
        lambda d: re.match('.*BUNNIES.*', d.name),
        lambda d: re.match('.*/None.*', d.name),
        lambda d: re.match('.*FAKE.*', d.name)
    ]
)

registry = Configuration(
    db_params = {
        'config_file': '/etc/my.cnf',
        'config_group': 'mysql-dynamo',
        'db': 'dynamoregister'
    }
)
