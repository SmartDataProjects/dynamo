from common.interface.classes import default_interface
from common.dataformat import DatasetDemand

class DemandManager(object):
    """
    Aggregate information from multiple sources and create a Demand object for a dataset.
    """

    def __init__(self, load_data = True, popularity_cls = None, lock_cls = None):
        if popularity_cls:
            self.popularity = popularity_cls()
        else:
            self.popularity = default_interface['popularity']()

        if lock_cls:
            self.lock = lock_cls()
        else:
            self.lock = default_interface['lock']()

        if load_data:
            self.load()

    def load(self):
        pass

    def update(self, inventory):
        pass

    def get_demand(self, dataset):
        return DatasetDemand(dataset, 100.)
