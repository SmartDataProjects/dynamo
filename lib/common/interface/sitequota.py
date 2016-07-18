from common.interface.mysql import MySQL

class SiteQuotaRetriever(object):
    """
    A temporary interface to IntelROCCS site quota database.
    Also get site activity status (set by hand).
    """

    def __init__(self):
        self._mysql = MySQL(config_file = '/etc/my.cnf', config_group = 'mysql-ddm', db = 'IntelROCCS')

    def get_quota(self, site, group):
        entry = self._mysql.query('SELECT q.`SizeTb` FROM `Quotas` AS q INNER JOIN `Sites` AS s ON s.`SiteId` = q.`SiteId` INNER JOIN `Groups` AS g ON g.`GroupId` = q.`GroupId` WHERE s.`SiteName` LIKE %s AND g.`GroupName` LIKE %s ORDER BY q.`EntryDateUT` DESC LIMIT 1', site.name, group.name)

        if len(entry) == 0:
            return 0

        return entry[0]

    def get_status(self, site):
        entry = self._mysql.query('SELECT `Status` FROM `Sites` WHERE `SiteName` LIKE %s', site.name)

        if len(entry) == 0:
            return 0

        return entry[0]
