from phedex import PhedexInterface
from mysql import MySQLInterface

phdx = PhedexInterface()
msql = MySQLInterface()

default_interface = {
    'status_probe': phdx,
    'transfers': phdx,
    'deletion': phdx,
    'inventory': msql
}
