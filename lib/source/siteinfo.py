import fnmatch
import re
import logging

from dynamo.utils.classutil import get_instance
from dynamo.dataformat import Configuration

LOG = logging.getLogger(__name__)

class SiteInfoSource(object):
    """
    Interface specs for probe to the site information source.
    """

    @staticmethod
    def get_instance(module = None, config = None):
        if module is None:
            module = SiteInfoSource._module
        if config is None:
            config = SiteInfoSource._config

        return get_instance(SiteInfoSource, module, config)

    _module = ''
    _config = Configuration()

    @staticmethod
    def set_default(config):
        SiteInfoSource._module = config.module
        SiteInfoSource._config = config.config

    def __init__(self, config):
        if hasattr(config, 'include'):
            if type(config.include) is list:
                self.include = map(lambda pattern: re.compile(fnmatch.translate(pattern)), config.include)
            else:
                self.include = [re.compile(fnmatch.translate(config.include))]
        else:
            self.include = None

        if hasattr(config, 'exclude'):
            if type(config.exclude) is list:
                self.exclude = map(lambda pattern: re.compile(fnmatch.translate(pattern)), config.exclude)
            else:
                self.exclude = [re.compile(fnmatch.translate(config.exclude))]
        else:
            self.exclude = None

    def get_site(self, name, inventory):
        """
        @param name  Name of the site
        @return  A Site object with full info, or None if the site is not found.
        """
        raise NotImplementedError('get_site')

    def get_site_list(self, inventory):
        """
        @return List of unlinked Site objects
        """
        raise NotImplementedError('get_site_list')

    def get_site_status(self, site_name):
        """
        @param site_name  Site name
        """
        raise NotImplementedError('get_site_status')

    def get_filename_mapping(self, site_name):
        """
        Get the list of regular expression file name mapping rules for the given site.
        @param site_name  Site name

        @return {protocol: chains} where chains = [chain] and chain = [(match, dest), (match, dest)]
        """
        raise NotImplementedError('get_filename_mapping')

    def check_allowed_site(self, site_name):
        if self.include is not None:
            for pattern in self.include:
                if pattern.match(site_name):
                    break
            else:
                # no match
                LOG.debug('Site %s is not in include list.', site_name)
                return False

        if self.exclude is not None:
            for pattern in self.exclude:
                if pattern.match(site_name):
                    LOG.debug('Site %s is in exclude list.', site_name)
                    return False

        return True
