class DemandManager(object):
    """
    This class is just a holder for "demand calculator" plugins. The plugins must have functions
    load() and update(), which take an InventoryManager as the sole argument.
    """

    def __init__(self, config):
        self.calculators = {} # name -> demand calculator

    def load(self, inventory, enabled_calculators):
        """
        Loop over the plugins and call load().
        @param inventory            An InventoryManager instance.
        @param enabled_calculators  List of plugin names.
        """

        for dataset in inventory.datasets.values():
            dataset.demand = {}

        for cname in enabled_calculators:
            self.calculators[cname].load(inventory)

    def update(self, inventory, enabled_calculators):
        """
        Loop over the plugins and call update().
        @param inventory            An InventoryManager instance.
        @param enabled_calculators  List of plugin names.
        """

        for cname in enabled_calculators:
            self.calculators[cname].update(inventory)
