from common.interface.mysql import MySQL
from common.dataformat import Site

class SiteQuotaRetriever(object):
    """
    A temporary interface to IntelROCCS site quota database.
    Also get site activity status (set by hand).
    """

    def __init__(self):
        self._mysql = MySQL(config_file = '/etc/my.cnf', config_group = 'mysql-ddm', db = 'IntelROCCS')

    def get_quota(self, site, partition):
        partition_names = [partition.name]

        if partition_names[0] == 'Physics':
            partition_names = ['AnalysisOps', 'DataOps']

        elif partition_names[0] == 'IB RelVal':
            # IntelROCCS replaces IB RelVal with IB-RelVal
            partition_names[0] = 'IB-RelVal'

        quota = 0.

        for partition_name in partition_names:
            entry = self._mysql.query('SELECT q.`SizeTb` FROM `Quotas` AS q INNER JOIN `Sites` AS s ON s.`SiteId` = q.`SiteId` INNER JOIN `Groups` AS g ON g.`GroupId` = q.`GroupId` WHERE s.`SiteName` LIKE %s AND g.`GroupName` LIKE %s ORDER BY q.`EntryDateUT` DESC LIMIT 1', site.name, partition_name)

            if len(entry) != 0:
                quota += entry[0]

        return quota

    def get_status(self, site):
#        if site.storage_type == Site.TYPE_MSS:
#            return Site.ACT_NOCOPY

        entry = self._mysql.query('SELECT `Status` FROM `Sites` WHERE `SiteName` LIKE %s', site.name)

        if len(entry) == 0:
            return Site.ACT_IGNORE

        return entry[0]
