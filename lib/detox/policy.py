import logging
import collections

logger = logging.getLogger(__name__)

class Policy(object):
    """
    Responsible for partitioning the replicas, setting quotas and activating deletion on sites, and making deletion decisions on replicas.
    The core of the object is a stack of rules (specific rules first) with a fall-back default decision.
    A rule is a callable object with (replica, demand_manager) as arguments that returns None or (replica, decision, reason)
    """

    # do not change order - used by history records
    DEC_NEUTRAL, DEC_DELETE, DEC_KEEP, DEC_PROTECT = range(1, 5)
    DECISION_STR = {DEC_NEUTRAL: 'NEUTRAL', DEC_DELETE: 'DELETE', DEC_KEEP: 'KEEP', DEC_PROTECT: 'PROTECT'}

    def __init__(self, default, stack, quotas, site_requirement, prerequisite = None):
        self.default_decision = default # decision
        self.stack = stack # [rule]
        self.quotas = quotas # {site: quota}
        self.site_requirement = site_requirement # bool(site)
        self.prerequisite = prerequisite # bool(replica)

    def applies(self, replica):
        if self.prerequisite is None:
            return True
        else:
            return self.prerequisite(replica)

    def need_deletion(self, site):
        return self.site_requirement(site)

    def evaluate(self, replica, demand_manager):
        for rule in self.rules:
            result = rule(replica, demand_manager)
            if result is not None:
                break
        else:
            return self.default_decision, 'Policy default'

        return result
