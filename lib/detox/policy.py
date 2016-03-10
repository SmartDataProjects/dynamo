class DeletionPolicy(object):
    """
    Base class for deletion policies.
    """

    DEC_KEEP = 0
    DEC_DELETE = 1
    DEC_KEEP_OVERRIDE = 2

    def decision(self, replica, demand):
        return DEC_KEEP


class DeletionPolicyManager(object):
    """
    Holds a stack of deletion policies and make a collective decision on a replica.
    """

    def __init__(self):
        self._policies = []

    def decision(self, replica, demand):
        """
        Loop over the policies. Return DELETE if at least one policy hits, unless
        there is a KEEP_OVERRIDE.
        """
        
        result = DeletionPolicy.DEC_KEEP

        for policy in self._policies:
            dec = policy.decision(replica, demand)
            if dec == DeletionPolicy.DEC_DELETE and result == DeletionPolicy.DEC_KEEP:
                result = DeletionPolicy.DEC_DELETE

            elif dec == DeletionPolicy.DEC_KEEP_OVERRIDE:
                result = DeletionPolicy.DEC_KEEP

        # TODO Add function to record policy hits

        return result
