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

    def get_replicas(self, site = None, dataset = None, block = None):
        """
        Return a list of unlinked BlockReplicas matching the given name patterns.
        @param site    Site name (wildcard allowed) or None
        @param dataset Dataset name (wildcard allowed) or None
        @param block   Block name (wildcard allowed) or None
        """
        raise NotImplementedError('get_replicas')

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
