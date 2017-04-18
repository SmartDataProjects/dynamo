class BaseHandler(object):
    def __init__(self, name):
        self.name = name

    def get_requests(self, inventory, partition):
        """
        Return datasets, blocks, files, all sorted by priority.
        """

        return [], [], []

    def save_record(self, run_number, history, copy_list):
        pass
