class Configuration(object):
    pass

debug_level = 0

mysql = Configuration()
mysql.db = 'DDM_devel'
mysql.host = 'localhost'
mysql.user = 'ddmdevel'
mysql.passwd = 'intelroccs'

phedex = Configuration()
phedex.x509_key = '/tmp/x509up_u51268'
phedex.url_base = 'https://cmsweb.cern.ch/phedex/datasvc/json/prod'
