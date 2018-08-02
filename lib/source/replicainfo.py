import logging
import fnmatch
import re

from dynamo.dataformat import Configuration
from dynamo.utils.classutil import get_instance

LOG = logging.getLogger(__name__ )

class ReplicaInfoSource(object):
    """
    Interface specs for probe to the replica information source.
    """

    @staticmethod
    def get_instance(module = None, config = None):
        if module is None:
            module = ReplicaInfoSource._module
        if config is None:
            config = ReplicaInfoSource._config

        return get_instance(ReplicaInfoSource, module, config)

    # defaults
    _module = ''
    _config = Configuration()

    @staticmethod
    def set_default(config):
        ReplicaInfoSource._module = config.module
        ReplicaInfoSource._config = config.config

    def __init__(self, config = None):
        config = Configuration(config)

        if 'include_datasets' in config:
            if type(config.include_datasets) is list:
                self.include_datasets = map(lambda pattern: re.compile(fnmatch.translate(pattern)), config.include_datasets)
            else:
                self.include_datasets = [re.compile(fnmatch.translate(config.include_datasets))]
        else:
            self.include_datasets = None

        if 'exclude_datasets' in config:
            if type(config.exclude_datasets) is list:
                self.exclude_datasets = map(lambda pattern: re.compile(fnmatch.translate(pattern)), config.exclude_datasets)
            else:
                self.exclude_datasets = [re.compile(fnmatch.translate(config.exclude_datasets))]
        else:
            self.exclude_datasets = None

        if 'include_sites' in config:
            if type(config.include_sites) is list:
                self.include_sites = map(lambda pattern: re.compile(fnmatch.translate(pattern)), config.include_sites)
            else:
                self.include_sites = [re.compile(fnmatch.translate(config.include_sites))]
        else:
            self.include_sites = None

        if 'exclude_sites' in config:
            if type(config.exclude_sites) is list:
                self.exclude_sites = map(lambda pattern: re.compile(fnmatch.translate(pattern)), config.exclude_sites)
            else:
                self.exclude_sites = [re.compile(fnmatch.translate(config.exclude_sites))]
        else:
            self.exclude_sites = None

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

    def get_updated_replicas(self, updated_since, inventory):
        """
        Return a list of unlinked BlockReplicas updated since the given timestamp.
        """
        raise NotImplementedError('get_updated_replicas')

    def get_deleted_replicas(self, deleted_since):
        """
        Return a list of unlinked BlockReplicas deleted since the given timestamp.
        """
        raise NotImplementedError('get_deleted_replicas')

    def check_allowed_dataset(self, dataset_name):
        """
        Check a dataset name against include and exclude lists.
        """

        if self.include_datasets is not None:
            for pattern in self.include_datasets:
                if pattern.match(dataset_name):
                    break
            else:
                # no pattern matched
                LOG.debug('Dataset %s is not in include list.', dataset_name)
                return False

        if self.exclude_datasets is not None:
            for pattern in self.exclude_datasets:
                if pattern.match(dataset_name):
                    LOG.debug('Dataset %s is in exclude list.', dataset_name)
                    return False

        return True

    def check_allowed_site(self, site_name):
        """
        Check a site name against include and exclude lists.
        """

        if self.include_sites is not None:
            for pattern in self.include_sites:
                if pattern.match(site_name):
                    break
            else:
                # no pattern matched
                LOG.debug('Site %s is not in include list.', site_name)
                return False

        if self.exclude_sites is not None:
            for pattern in self.exclude_sites:
                if pattern.match(site_name):
                    LOG.debug('Site %s is in exclude list.', site_name)
                    return False

        return True
