from policy.condition import Condition

class ReplicaCondition(Condition):
    def get_variable(self, expr): # override
        """Return a tuple containing (callable variable definition, variable type, ...)"""

        return variables.replica_variables[expr]

    def get_matching_blocks(self, replica):
        """If this is a block-level condition, return the list of matching block replicas."""

        matching_blocks = []
        for block_replica in replica.block_replicas:
            if self.match(block_replica):
                matching_blocks.append(block_replica)

        return matching_blocks

class SiteCondition(Condition):
    def __init__(self, text, partition):
        self.partition = partition

        Condition.__init__(self, text)

    def get_variable(self, expr): # override
        """Return a tuple containing (callable variable definition, variable type, ...)"""

        variable = variables.site_variables[expr]
        variable.partition = self.partition

        return variable
