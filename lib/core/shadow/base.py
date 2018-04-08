class MasterShadow(object):
    def __init__(self, config):
        pass

    def copy(self, master_server):
        """
        Download data from master server and write a local backup.
        """
        pass

    def get_next_master(self, current):
        """
        @param current  Current master server host name.

        @return  (hostname, module, config)
        """
        pass
