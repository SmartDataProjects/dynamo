from common.interface.defaults import default_interface
from common.dataformat import IntegrityError

class InventoryManager(object):
    """Bookkeeping class to bridge the communication between remote and local data sources."""

    def __init__(self):
        self.local_data = default_interface['inventory']
        self.data_source = default_interface['status_probe']

    def update(self):
        """Query the dataSource and get updated information."""

        # Lock the inventory
        self.local_data.acquire_lock()

        try:
            # We start fresh and write all replica information in, instead of updating them.
            # Site, dataset, and block information are kept.
            self.local_data.make_snapshot()
            self.local_data.prepare_new()

            sites = self.data_source.get_sites()
            self.local_data.update_site_list(sites)
    
            datasets = self.data_source.get_datasets()
            self.local_data.update_dataset_list(datasets)

            blocks = []

            for dataset in datasets:
                # Write to dataset replica table
                self.local_data.place_dataset(dataset.replicas)

                # For partial replicas, deal with block replicas
                for drep in dataset.replicas:
                    if not drep.is_partial:
                        continue
                    
                    # List of block replicas on the site where the dataset is partial
                    site_replicas = []
                    for block in dataset.blocks:
                        for brep in block.replicas:
                            if brep.site != drep.site:
                                continue
                            
                            # Replica exists on the site
                            site_replicas.append(brep)
                            blocks.append(block)
                            break

                    self.local_data.place_block(site_replicas)

            self.local_data.update_block_list(blocks)

        finally:
            # Lock is released even in case of unexpected errors
            self.local_data.release_lock()

    def find_data(self):
        """Query the local DB for datasets/blocks."""
        pass

    def commit(self):
        """Commit the updates into the local DB. Might not be necessary
        if diff information is not needed."""
        pass
