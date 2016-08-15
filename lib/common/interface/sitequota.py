from common.interface.mysql import MySQL
from common.dataformat import Site

class SiteQuotaRetriever(object):
    """
    A temporary interface to IntelROCCS site quota database.
    Also get site activity status (set by hand).
    """

    def __init__(self):
        self._mysql = MySQL(config_file = '/etc/my.cnf', config_group = 'mysql-ddm', db = 'IntelROCCS')

    def get_quota(self, site, group):
        if site.name.endswith('_MSS'):
            return 1000000

        group_name = group.name
        # IntelROCCS replaces IB RelVal with IB-RelVal
        if group_name == 'IB RelVal':
            group_name = 'IB-RelVal'

        entry = self._mysql.query('SELECT q.`SizeTb` FROM `Quotas` AS q INNER JOIN `Sites` AS s ON s.`SiteId` = q.`SiteId` INNER JOIN `Groups` AS g ON g.`GroupId` = q.`GroupId` WHERE s.`SiteName` LIKE %s AND g.`GroupName` LIKE %s ORDER BY q.`EntryDateUT` DESC LIMIT 1', site.name, group_name)

        if len(entry) == 0:
            return 0

        return entry[0]

    def get_status(self, site):
        if site.name.endswith('_MSS'):
            return Site.ACT_NOCOPY

        entry = self._mysql.query('SELECT `Status` FROM `Sites` WHERE `SiteName` LIKE %s', site.name)

        if len(entry) == 0:
            return Site.ACT_IGNORE

        return entry[0]
