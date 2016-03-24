import logging
import collections

logger = logging.getLogger(__name__)

class Policy(object):
    """
    Base class for policies.
    """

    DEC_KEEP, DEC_DELETE, DEC_KEEP_OVERRIDE = range(3)

    def __init__(self, name):
        """
        Base class for deletion policies.
        """

        self.name = name

    def decision(self, replica, demand_manager, records = None):
        """
        Not intended for overrides. Decide if the policy applies to a given replica under the current demands. If applies, call case_match. If not, return DEC_KEEP.
        """

        logger.debug('Testing whether %s applies to %s:%s', self.name, replica.site.name, replica.dataset.name)
        applies, reason = self.applies(replica, demand_manager)

        if applies:
            dec = self.case_match(replica)

            if records:
                records.add_record(self, dec, reason = reason)

            return dec

        else:
            return Policy.DEC_KEEP

    def applies(self, replica, demand_manager):
        """
        To be overridden by subclasses. Return a boolean and a string explaining the decision.
        """

        return False, ''

    def case_match(self, replica):
        """
        Usually returns a fixed value; can be made dynamic if necessary. To be overridden by subclasses
        """

        return Policy.DEC_KEEP


class DeletePolicy(Policy):
    """
    Base class for policies with case_match = DEC_DELETE.
    """

    def case_match(self, replica): # override
        return Policy.DEC_DELETE


class KeepPolicy(Policy):
    """
    Base class for policies with case_match = DEC_KEEP_OVERRIDE.
    """

    def case_match(self, replica): # override
        return Policy.DEC_KEEP_OVERRIDE


class PolicyHitRecords(object):
    """
    Helper class to record policy decisions on each replica.
    """

    Record = collections.namedtuple('Record', ['policy', 'decision', 'reason'])

    def __init__(self, replica):
        self.replica = replica
        self.records = []

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
            if decision == Policy.DEC_DELETE:
                decision_str = 'DELETE'
            elif decision == Policy.DEC_KEEP_OVERRIDE:
                decision_str = 'KEEP_OVERRIDE'

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

    def add_policy(self, policy):
        if type(policy) is list:
            self._policies += policy
        else:
            self._policies.append(policy)

    def decision(self, replica, demand):
        """
        Loop over the policies. Return DELETE if at least one policy hits, unless
        there is a KEEP_OVERRIDE.
        """
        
        result = Policy.DEC_KEEP

        hit_records = PolicyHitRecords(replica)

        for policy in self._policies:
            dec = policy.decision(replica, demand, hit_records)
            if dec == Policy.DEC_DELETE:
                result = Policy.DEC_DELETE

            elif dec == Policy.DEC_KEEP_OVERRIDE:
                return Policy.DEC_KEEP, hit_records

        return result, hit_records
