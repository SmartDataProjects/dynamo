class BaseHandler(object):
    def __init__(self, name):
        self.name = name
        self.used_demand_plugins = []

    def get_requests(self, inventory, partition):
        """
        Return datasets, blocks, files, all sorted by priority.
        """

        return [], [], []

    def save_record(self, run_number, history, copy_list):
        pass
