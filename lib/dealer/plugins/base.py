class BaseHandler(object):
    def __init__(self, name):
        self.name = name
        self.required_attrs = []

    def get_requests(self, inventory, history, policy):
        """
        Return a prioritized list of objects requesting transfer of.
        """

        return []

    def save_record(self, run_number, history, copy_list):
        pass
