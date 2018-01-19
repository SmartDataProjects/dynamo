from dynamo.policy.condition import Condition
from dynamo.policy.variables import site_variables, replica_variables

class ReplicaCondition(Condition):
    def __init__(self, text):
        Condition.__init__(self, text, replica_variables)

    def get_matching_blocks(self, replica):
        """If this is a block-level condition, return the list of matching block replicas."""

        matching_blocks = []
        for block_replica in replica.block_replicas:
            if self.match(block_replica):
                matching_blocks.append(block_replica)

        return matching_blocks

class SiteCondition(Condition):
    def __init__(self, text):
        Condition.__init__(self, text, site_variables)
