class SiteInfoSourceInterface(object):
    """
    Interface specs for probe to the site information source.
    """

    def __init__(self, config):
        pass

    def get_site_list(self):
        """
        Return a list of unlinked site objects.
        """
        raise NotImplementedError('get_site_list')

    def get_site_status(self, site):
        """
        Return the site status.
        @param site  Site object
        @returns Site.STAT_X flag
        """
        raise NotImplementedError('get_site_status')
