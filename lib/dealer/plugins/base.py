class BaseHandler(object):
    def __init__(self, name):
        self.name = name
        self.used_demand_plugins = []

    def get_requests(self, inventory, policy, target_sites):
        """
        Return a prioritized list of objects requesting transfer of.
        """

        return []

    def save_record(self, run_number, history, copy_list):
        pass
