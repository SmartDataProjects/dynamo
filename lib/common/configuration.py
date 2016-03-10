import logging

class Configuration(object):
    pass

logging.basicConfig(level = logging.INFO)

webservice = Configuration()
webservice.x509_key = '/tmp/x509up_u51268'

mysql = Configuration()
mysql.db = 'DDM_devel'
mysql.host = 'localhost'
mysql.user = 'ddmdevel'
mysql.passwd = 'intelroccs'

phedex = Configuration()
phedex.url_base = 'https://cmsweb.cern.ch/phedex/datasvc/json/prod'

dbs = Configuration()
dbs.url_base = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'

inventory = Configuration()
inventory.refresh_min = 7200
inventory.included_sites = ['T2_*', 'T1_*_Disk']
inventory.included_groups = ['AnalysisOps']
