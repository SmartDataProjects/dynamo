import logging
import collections

logger = logging.getLogger(__name__)

DEC_NEUTRAL, DEC_DELETE, DEC_KEEP, DEC_PROTECT = range(4)
DECISIONS = range(4)
DECISION_STR = {DEC_NEUTRAL: 'NEUTRAL', DEC_DELETE: 'DELETE', DEC_KEEP: 'KEEP', DEC_PROTECT: 'PROTECT'}

class Policy(object):
    """
    Base class for policies.
    """

    def __init__(self, name):
        """
        Base class for deletion policies.
        """

        self.name = name

    def decision(self, replica, demand_manager, records = None):
        """
        Not intended for overrides. Decide if the policy applies to a given replica under the current demands. If applies, call case_match. If not, return DEC_NEUTRAL.
        """

        applies, reason = self.applies(replica, demand_manager)

        if applies:
            dec = self.case_match(replica)

            if records:
                records.add_record(self, dec, reason = reason)

            return dec

        else:
            return DEC_NEUTRAL

    def applies(self, replica, demand_manager):
        """
        To be overridden by subclasses. Return a boolean and a string explaining the decision.
        """

        return False, ''

    def case_match(self, replica):
        """
        Usually returns a fixed value; can be made dynamic if necessary. To be overridden by subclasses
        """

        return DEC_NEUTRAL


class DeletePolicy(Policy):
    """
    Base class for policies with case_match = DEC_DELETE.
    """

    def case_match(self, replica): # override
        return DEC_DELETE


class KeepPolicy(Policy):
    """
    Base class for policies with case_match = DEC_KEEP.
    """

    def case_match(self, replica): # override
        return DEC_KEEP


class ProtectPolicy(Policy):
    """
    Base class for policies with case_match = DEC_PROTECT.
    """

    def case_match(self, replica): # override
        return DEC_PROTECT


class PolicyHitRecords(object):
    """
    Helper class to record policy decisions on each replica.
    """

    Record = collections.namedtuple('Record', ['policy', 'decision', 'reason'])

    def __init__(self, replica):
        self.replica = replica
        self.records = []

    def decision(self):
        result = DEC_NEUTRAL

        for record in self.records:
            if record.decision == DEC_DELETE and result == DEC_NEUTRAL:
                result = DEC_DELETE

            elif record.decision == DEC_KEEP and (result == DEC_NEUTRAL or result == DEC_DELETE):
                result = DEC_KEEP

            elif record.decision == DEC_PROTECT:
                result = DEC_PROTECT

        return result

    def add_record(self, policy, decision, reason = ''):
        record = PolicyHitRecords.Record(policy, decision, reason)
        self.records.append(record)

    def write_records(self, output):
        output.write('Policy hits for replica {site} {dataset}:'.format(site = self.replica.site.name, dataset = self.replica.dataset.name))
        if len(self.records) == 0:
            output.write(' None\n')
        else:
            output.write('\n')

        for policy, decision, reason in self.records:
            if decision == DEC_DELETE:
                decision_str = 'DELETE'
            elif decision == DEC_KEEP:
                decision_str = 'KEEP'
            elif decision == DEC_PROTECT:
                decision_str = 'PROTECT'

            line = '{policy}: {decision}'.format(policy = policy.name, decision = decision_str)
            if reason:
                line += ' (%s)' % reason

            output.write(' ' + line + '\n')


class PolicyManager(object):
    """
    Holds a stack of deletion policies and make a collective decision on a replica.
    """

    def __init__(self, policies):
        self._policies = policies

    def num_policies(self):
        return len(self._policies)

    def add_policy(self, policy):
        if type(policy) is list:
            self._policies += policy
        else:
            self._policies.append(policy)

    def decision(self, replica, demand):
        """
        Loop over the policies. Return DELETE if at least one policy hits, unless
        there is a PROTECT.
        """
        
        result = DEC_NEUTRAL

        hit_records = PolicyHitRecords(replica)

        for policy in self._policies:
#            if logger.getEffectiveLevel() == logging.DEBUG:
#                logger.debug('Testing %s:%s against %s', replica.site.name, replica.dataset.name, policy.name)

            policy.decision(replica, demand, hit_records)

        return hit_records
