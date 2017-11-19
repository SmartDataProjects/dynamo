class SiteInfoSource(object):
    """
    Interface specs for probe to the site information source.
    """

    def __init__(self, config):
        if hasattr(config, 'include'):
            if type(config.include) is list:
                self.include = list(config.include)
            else:
                self.include = [config.include]
        else:
            self.include = None

        if hasattr(config, 'exclude'):
            if type(config.exclude) is list:
                self.exclude = list(config.exclude)
            else:
                self.exclude = [config.exclude]
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

    def set_site_properties(self, site):
        """
        @param site  Site object
        """
        raise NotImplementedError('get_site_status')
