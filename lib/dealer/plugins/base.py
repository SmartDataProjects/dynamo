class BaseHandler(object):
    def __init__(self, name):
        self.name = name
        self.required_attrs = []

    def get_requests(self, inventory, history, policy):
        """
        Return a prioritized list of objects requesting transfer of.
        """

        return []

    def postprocess(self, cycle_number, history, copy_list):
        """
        Do post-request processing.
        @param cycle_number   Dealer cycle number
        @param history        History object
        @param copy_list      List of DatasetReplicas
        """
        pass
