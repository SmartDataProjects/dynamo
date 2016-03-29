import time
import re
import fnmatch

import detox.policy as policy
import detox.configuration as detox_config

class ProtectIncomplete(policy.ProtectPolicy):
    """
    PROTECT if the replica is not complete.
    """
    
    def __init__(self, name = 'ProtectIncomplete'):
        super(self.__class__, self).__init__(name)

    def applies(self, replica, demand_manager): # override
        return not replica.is_complete, 'Replica is not complete.'


class ProtectLocked(policy.ProtectPolicy):
    """
    PROTECT if any block of the dataset is locked.
    """

    def __init__(self, name = 'ProtectLocked'):
        super(self.__class__, self).__init__(name)

    def applies(self, replica, demand_manager): # override
        all_blocks = set([b.name for b in replica.dataset.blocks])
        locked_blocks = set(demand_manager.get_demand(replica.dataset).locked_blocks)

        intersection = all_blocks & locked_blocks
        reason = 'Blocks locked: ' + ' '.join(intersection)
        
        return len(intersection) != 0, reason


class ProtectCustodial(policy.ProtectPolicy):
    """
    PROTECT if the replica is custodial.
    """

    def __init__(self, name = 'ProtectCustodial'):
        super(self.__class__, self).__init__(name)

    def applies(self, replica, demand_manager): # override
        return replica.is_custodial, 'Replica is custodial.'


class ProtectDiskOnly(policy.ProtectPolicy):
    """
    PROTECT if the dataset is not on tape. 
    """

    def __init__(self, name = 'ProtectDiskOnly'):
        super(self.__class__, self).__init__(name)

    def applies(self, replica, demand_manager): # override
        return not replica.dataset.on_tape, 'Replica is a last copy with no tape copy.'


class ProtectMinimumCopies(policy.ProtectPolicy):
    """
    PROTECT if the dataset has only minimum number of replicas.
    """
    
    def __init__(self, name = 'ProtectMinimumCopies'):
        super(self.__class__, self).__init__(name)

    def applies(self, replica, demand_manager): # override
        return len(replica.dataset.replicas) <= demand_manager.get_demand(replica.dataset).required_copies


class KeepTargetOccupancy(policy.KeepPolicy):
    """
    PROTECT if occupancy of the replica's site is less than a set target.
    """

    def __init__(self, threshold, name = 'ProtectTargetOccupancy'):
        super(self.__class__, self).__init__(name)

        self.threshold = threshold

    def applies(self, replica, demand_manager): # override
        return replica.site.occupancy() < self.threshold, 'Site is underused.'


class DeletePartial(policy.DeletePolicy):
    """
    DELETE if the replica is partial.
    """

    def __init__(self, name = 'DeletePartial'):
        super(self.__class__, self).__init__(name)

    def applies(self, replica, demand_manager): # override
        return replica.is_partial, 'Replica is partial.'


class DeleteOld(policy.DeletePolicy):
    """
    DELETE if the replica is older than a set time from now.
    """

    def __init__(self, threshold, unit, name = 'DeleteOld'):
        super(self.__class__, self).__init__(name)

        self.threshold = threshold
        if unit == 'y':
            self.threshold *= 365.
        if unit == 'y' or unit == 'd':
            self.threshold *= 24.
        if unit == 'y' or unit == 'd' or unit == 'h':
            self.threshold *= 3600.

        self.threshold_text = '%f%s' % (threshold, unit)

    def applies(self, replica, demand_manager): # override
        if replica.dataset.last_accessed <= 0:
            return False, ''

        return replica.dataset.last_accessed < time.time() - self.threshold, 'Replica is older than ' + self.threshold_text + '.'


class DeleteUnpopular(policy.DeletePolicy):
    """
    DELETE if this is less popular than a threshold or is the least popular dataset at the site.
    """

    def __init__(self, name = 'DeleteUnpopular'):
        super(self.__class__, self).__init__(name)

        self.threshold = detox_config.delete_unpopular.threshold

    def applies(self, replica, demand_manager): # override
        score = demand_manager.get_demand(replica.dataset).popularity_score

        if score > self.threshold:
            return True, 'Dataset is less popular than threshold.'

        max_site_score = max([demand_manager.get_demand(d).popularity_score for d in replica.site.datasets])

        return score >= max_site_score, 'Dataset is the least popular on the site.'


class ActionList(policy.Policy):
    """
    Take decision from a list of 
    The list should have a decision, a site, and a dataset (wildcard allowed for both) per row, separated by white spaces.
    Any line that does not match the pattern
      (Keep|Delete) <site> <dataset>
    is ignored.
    """

    def __init__(self, list_path = '', name = 'ActionList'):
        super(self.__class__, self).__init__(name)

        self.res = [] # (site_re, dataset_re, action)
        self.patterns = [] # (site_pattern, dataset_pattern)
        self.actions = {} # replica -> action

        if list_path:
            self.load_list(list_path)

    def load_list(self, list_path):
        with open(list_path) as deletion_list:
            for line in deletion_list:
                matches = re.match('\s*(Keep|Delete)\s+([A-Za-z0-9_*]+)\s+(/[\w*-]+/[\w*-]+/[\w*-]+)', line.strip())
                if not matches:
                    continue

                action_str = matches.group(1)
                site_pattern = matches.group(2)
                dataset_pattern = matches.group(3)

                site_re = re.compile(fnmatch.translate(site_pattern))
                dataset_re = re.compile(fnmatch.translate(dataset_pattern))

                if action_str == 'Keep':
                    action = policy.DEC_PROTECT
                else:
                    action = policy.DEC_DELETE

                self.res.append((site_re, dataset_re, action))
                self.patterns.append((site_pattern, dataset_pattern))

    def applies(self, replica, demand_manager): # override
        """
        Loop over the patterns list and make an entry in self.actions if the pattern matches.
        """

        for iL, (site_re, dataset_re, action) in enumerate(self.res):
            if site_re.match(replica.site.name) and dataset_re.match(replica.dataset.name):
                self.actions[replica] = action
                return True, 'Pattern match: site=%s, dataset=%s' % self.patterns[iL]

        return False, ''
    
    def case_match(self, replica): # override
        return self.actions[replica]


def make_stack(strategy):
    if strategy == 'TargetFraction':
        stack = [
            KeepTargetOccupancy(detox_config.keep_target.occupancy),
            ProtectIncomplete(),
            ProtectLocked(),
            ProtectCustodial(),
            ProtectDiskOnly(),
#            DeletePartial(),
            DeleteOld(*detox_config.delete_old.threshold),
#            DeleteUnpopular()
        ]

    elif strategy == 'List':
        stack = [
            ProtectIncomplete(),
            ProtectLocked(),
            ProtectCustodial(),
            ProtectDiskOnly(),
            ActionList()
        ]

    return stack
