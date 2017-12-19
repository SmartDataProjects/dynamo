import time
import datetime
import collections

from utils.interface.popdb import PopDB
from utils.interface.mysql import MySQL

# last_access is unix time
DatasetReplicaUsage = collections.namedtuple('DatasetReplicaUsage', ['rank', 'num_access', 'last_access'])

class CRABAccessHistory(object):
    """
    A plugin for DemandManager that sets up access history for dataset replicas.
    Sets two demand values:
      global_usage_rank:  float value
      local_usage:        {site: DatasetReplicaUsage}
    """

    def __init__(self, config):
        self._last_update = 0 # unix time of last update
        self.persistency = MySQLAccessHistoryStore({'db_params': {'db': 'dynamo'}})
        self._popdb = PopDB(config.popdb)

    def load(self, inventory):
        records = self.persistency.load_replica_accesses(inventory.sites.values(), inventory.datasets.values())
        self._last_update = records[0]

        self._compute(inventory, records[1])

    def update(self, inventory):
        # implemented in subclasses
        pass

    def _compute(self, inventory, access_list):
        """
        Set the dataset usage rank based on access list.
        Following the IntelROCCS implementation for local rank:
        datasetRank = (1-used)*(now-creationDate)/(60*60*24) + \
            used*( (now-lastAccessed)/(60*60*24)-nAccessed) - size/1000
        In the case of PopDB, nAccessed is NACC normalized by size (in GB).
        Argument access_list may only contain information on replicas that had
        zero access. We must loop over the datasets in inventory and set values
        for all datasets.
        """

        now = time.time()
        today = datetime.datetime.utcfromtimestamp(now).date()

        for dataset in inventory.datasets.values():
            local_usage = dataset.demand['local_usage'] = {} # {site: DatasetReplicaUsage}

            if dataset.replicas is None:
                continue

            for replica in dataset.replicas:
                try:
                    accesses = access_list[replica]
                except KeyError:
                    accesses = {}

                size = replica.size(physical = False) * 1.e-9

                if len(accesses) != 0:
                    last_access = max(accesses.keys())
                else:
                    last_access = datetime.datetime.min
                    
                num_access = sum(accesses.values())

                if num_access == 0:
                    local_rank = (now - replica.last_block_created()) / (24. * 3600.)
                elif size > 0.:
                    local_rank = (today - last_access).days - num_access / size
                else:
                    local_rank = (today - last_access).days

                local_rank -= size * 1.e-3

                # mktime returns expects the local time but the timetuple we pass is for UTC. subtracting time.timezone
                local_usage[replica.site] = DatasetReplicaUsage(local_rank, num_access, time.mktime(last_access.timetuple()) - time.timezone)

            global_rank = sum(usage.rank for usage in local_usage.values())

            if len(dataset.replicas) != 0:
                global_rank /= len(dataset.replicas)

            dataset.demand['global_usage_rank'] = global_rank

    def get_access_list(self, inventory, site_name, date):
        """
        Get the replica access data from PopDB.
        @param inventory  DynamoInventory
        @param site_name  Name of the site
        @param date       datetime.datetime instance
        @return  {replica: {date: (number of access, total cpu time)}}
        """
        
        if site_name.startswith('T0'):
            return {}
        elif site_name.startswith('T1') and site_name.count('_') > 2:
            nameparts = site_name.split('_')
            sitename = '_'.join(nameparts[:3])
            service = 'popularity/DSStatInTimeWindow/' # the trailing slash is apparently important
        elif site_name == 'T2_CH_CERN':
            sitename = site_name
            service = 'xrdpopularity/DSStatInTimeWindow'
        else:
            sitename = site_name
            service = 'popularity/DSStatInTimeWindow/'

        datestr = date.strftime('%Y-%m-%d')
        result = self._popdb.make_request(service, ['sitename=' + sitename, 'tstart=' + datestr, 'tstop=' + datestr])

        access_list = {}
        
        for ds_entry in result:
            try:
                dataset = inventory.datasets[ds_entry['COLLNAME']]
            except KeyError:
                continue

            replica = dataset.find_replica(site)
            if replica is None:
                continue

            if replica not in access_list:
                access_list[replica] = {}

            access_list[replica][date] = (int(ds_entry['NACC']), float(ds_entry['TOTCPU']))
