class ReplicaInfoSource(object):
    """
    Interface specs for probe to the replica information source.
    """

    def __init__(self, config):
        pass

    def replica_exists_at_site(self, site, item):
        """
        Query individual sites about individual items (dataset, block, or file)
        @param site  Site object
        @param item  Dataset, Block, or File object
        @return Boolean indicating whether a replica exists at the site.
        """
        raise NotImplementedError('replica_exists_at_site')

    def get_updated_replicas(self, updated_since):
        """
        Return a list of unlinked BlockReplicas updated since the given timestamp.
        """
        raise NotImplementedError('get_updated_replicas')

    def get_deleted_replicas(self, deleted_since):
        """
        Return a list of unlinked BlockReplicas deleted since the given timestamp.
        """
        raise NotImplementedError('get_deleted_replicas')
