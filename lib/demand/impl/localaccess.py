import time
import datetime
import collections
from common.interface.mysql import MySQL
import common.configuration as config

class LocalAccess(object):
    """
    A plugin
    """

    def __init__(self):
        self._last_update = 0 # unix time of last update
        self._mysqlreg = MySQL(**config.registry.db_params)
        self._mysqlhist = MySQL(**config.mysqlhistory.db_params)
        
    def load(self, inventory):
        print 'mmmmmmmmmmmmmmmmmmm'
        self._last_update = 0
        self._compute(inventory)

    def update(self, inventory):
        self._compute(inventory)
        # implemented in subclasses
        pass

    def _findUpdated(self,sql,sqlHandle,uptimes):
        responce = sqlHandle.query(sql)
        for (dsetName,updTime) in responce:
            if dsetName not in uptimes:
                uptimes[dsetName] = []
            uptimes[dsetName].append(updTime)


    def _compute(self,inventory):
        #here we look in the history of transfers and rank datasets
        #in terms of their recent copies, datasets with lowest rank
        #should be first candidates for deletion
        now = int(time.time())
        today = datetime.datetime.utcfromtimestamp(now).date()

        dset_uptimes = {}

        sql = "select item,updated from requests_unified where reqtype='copy' and"
        sql = sql + " updated > '" + str(today) + "' - INTERVAL 14 DAY" 
        self._findUpdated(sql,self._mysqlreg,dset_uptimes)

        sql = "select item,created from requests where reqtype='copy'"
        self._findUpdated(sql,self._mysqlreg,dset_uptimes)

        sql = "select name,updated from datasets inner join copy_dataset"
        sql = sql + " on datasets.id = copy_dataset.item_id"
        sql = sql + " where updated > '" + str(today) + "' - INTERVAL 14 DAY"
        self._findUpdated(sql,self._mysqlhist,dset_uptimes)

        for dataset in inventory.datasets.values():
            rank = 0.0
            if dataset.name in dset_uptimes:
                for updTime in dset_uptimes[dataset.name]:
                    dt = (today - updTime.date()).days
                    if dt > 0:
                        rank = rank + 14.0/dt
                    else:
                        rank = rank + 14.0

            dataset.demand['global_demand_rank'] = rank
