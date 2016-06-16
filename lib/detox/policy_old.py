import logging
import collections

logger = logging.getLogger(__name__)

# do not change order - used by history records
DEC_NEUTRAL, DEC_DELETE, DEC_KEEP, DEC_PROTECT = range(4)
DECISIONS = range(4)
DECISION_STR = {DEC_NEUTRAL: 'NEUTRAL', DEC_DELETE: 'DELETE', DEC_KEEP: 'KEEP', DEC_PROTECT: 'PROTECT'}

class Policy(object):
    """
    Base class for policies.
    """

    def __init__(self, name, static):
        """
        static: constant. Static policies are only evaluated once per run.
        """

        self.name = name
        self.static = static

    def decision(self, replica, demand_manager):
        """
        Not intended for overrides. Decide if the policy applies to a given replica under the current demands. If applies, call case_match. If not, return DEC_NEUTRAL.
        """

        applies, reason = self.applies(replica, demand_manager)

        if applies:
            dec = self.case_match(replica)
            return dec, reason

        else:
            return DEC_NEUTRAL, ''

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


class Evaluations(object):
    """
    Stores the policy hit records for a given replica.
    """

    class PolicyRecord(object):
        def __init__(self):
            self.evaluated = False
            self.decision = DEC_NEUTRAL
            self.reason = ''


    def __init__(self, replica, policy_stack):
        self.replica = replica
        self.records = []
        for policy in policy_stack:
            self.records.append((policy, Evaluations.PolicyRecord()))

    def run(self, demand):
        for policy, record in self.records:
            if record.evaluated:
                continue

            decision, reason = policy.decision(self.replica, demand)
            record.evaluated = policy.static
            record.decision = decision
            record.reason = reason

    def deciding_record(self):
        record = None

        for policy, rec in self.records:
            if rec.decision == DEC_PROTECT:
                return rec

            if record is None:
                record = rec

            elif rec.decision == DEC_DELETE and record.decision == DEC_NEUTRAL:
                record = rec

            elif rec.decision == DEC_KEEP and record.decision in (DEC_NEUTRAL, DEC_DELETE):
                record = rec

        return record

    def write_records(self, output):
        output.write('{site} {dataset}:\n'.format(site = self.replica.site.name, dataset = self.replica.dataset.name))
        if len(self.records) == 0:
            output.write(' None\n')
        else:
            for policy, record in self.records:
                if record.decision == DEC_DELETE:
                    decision_str = 'DELETE'
                elif record.decision == DEC_KEEP:
                    decision_str = 'KEEP'
                elif record.decision == DEC_PROTECT:
                    decision_str = 'PROTECT'
    
                line = ' {policy}: {decision}'.format(policy = policy.name, decision = decision_str)
                if record.reason:
                    line += ' (%s)' % record.reason
    
                output.write(line + '\n')
