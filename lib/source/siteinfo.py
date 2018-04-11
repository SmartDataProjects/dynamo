import fnmatch
import re
import logging

LOG = logging.getLogger(__name__)

class SiteInfoSource(object):
    """
    Interface specs for probe to the site information source.
    """

    @staticmethod
    def get_instance(module, config):
        import dynamo.source.impl as impl
        cls = getattr(impl, module)

        if not issubclass(cls, SiteInfoSource):
            raise RuntimeError('%s is not a subclass of SiteInfoSource' % module)

        return cls(config)


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

    def get_site(self, name):
        """
        @param name  Name of the site
        @return  A Site object with full info, or None if the site is not found.
        """
        raise NotImplementedError('get_site')

    def get_site_list(self):
        """
        @return List of unlinked Site objects
        """
        raise NotImplementedError('get_site_list')

    def get_site_status(self, site_name):
        """
        @param site_name  Site name
        """
        raise NotImplementedError('get_site_status')

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
