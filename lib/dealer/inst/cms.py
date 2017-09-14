from dealer.policy import ReplicaPlacementRule

class NoHIAtUSSites(ReplicaPlacementRule):
    def dataset_allowed(self, dataset, site):
        if '_US_' in site.name and site.name not in ['T2_US_MIT', 'T2_US_Vanderbilt']:
            if '/HI' in dataset.name or '/Hi' in dataset.name:
                return False

            if dataset.replicas is not None:
                for replica in dataset.replicas:
                    for block_replica in replica.block_replicas:
                        if block_replica.group.name == 'heavy-ions':
                            # if even one block replica belongs to HI, it must be a HI dataset
                            return False

        return True

    def block_allowed(self, block, site):
        return self.dataset_allowed(block.dataset, site)
