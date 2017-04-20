import time
import datetime
import collections

# last_access is unix time
DatasetReplicaUsage = collections.namedtuple('DatasetReplicaUsage', ['rank', 'num_access', 'last_access'])

class AccessHistory(object):
    """
    A plugin for DemandManager that sets up access history for dataset replicas.
    Sets two demand values:
      global_usage_rank:  float value
      local_usage:        {site: DatasetReplicaUsage}
    """

    def __init__(self):
        self._last_update = 0 # unix time of last update
        self._access_list = {} # {replica: {datetime.date: num_access}}

    def load(self, inventory):
        records = inventory.store.load_replica_accesses(inventory.sites.values(), inventory.datasets.values())
        self._last_update = records[0]
        self._access_list = records[1]

        self._compute(inventory)

    def update(self, inventory):
        # implemented in subclasses
        pass

    def _compute(self, inventory):
        """
        Set the dataset usage rank based on access list.
        Following the IntelROCCS implementation for local rank:
        datasetRank = (1-used)*(now-creationDate)/(60*60*24) + \
            used*( (now-lastAccessed)/(60*60*24)-nAccessed) - size/1000
        In the case of PopDB, nAccessed is NACC normalized by size (in GB).
        """

        now = time.time()
        today = datetime.datetime.utcfromtimestamp(now).date()

        for dataset in inventory.datasets.values():
            if dataset.replicas is None:
                continue

            dataset.demand['local_usage'] = {} # {site: DatasetReplicaUsage}
    
            global_rank = 0.

            for replica in dataset.replicas:
                try:
                    accesses = self._access_list[replica]
                except KeyError:
                    continue

                size = replica.size(physical = False) * 1.e-9

                last_access = max(accesses.keys())
                num_access = sum(accesses.values())

                if num_access == 0:
                    local_rank = (now - replica.last_block_created) / (24. * 3600.)
                else:
                    local_rank = (today - last_access).days - num_access / size

                local_rank -= size * 1.e-3

                # mktime returns expects the local time but the timetuple we pass is for UTC. subtracting time.timezone
                dataset.demand['local_usage'][replica.site] = DatasetReplicaUsage(local_rank, num_access, time.mktime(last_access.timetuple()) - time.timezone)

                global_rank += local_rank

            if len(dataset.replicas) != 0:
                global_rank /= len(dataset.replicas)

            dataset.demand['global_usage_rank'] = global_rank
