import time
import re
import fnmatch

from detox.policy import DeletePolicy, KeepPolicy
import detox.configuration as detox_config

class KeepIncomplete(KeepPolicy):
    """
    KEEP_OVERRIDE if the replica is not complete.
    """
    
    def __init__(self, name = 'KeepIncomplete'):
        super(self.__class__, self).__init__(name)

    def applies(self, replica, demand_manager): # override
        return not replica.is_complete, 'Replica is not complete.'


class KeepLocked(KeepPolicy):
    """
    KEEP_OVERRIDE if any block of the dataset is locked.
    """

    def __init__(self, name = 'KeepLocked'):
        super(self.__class__, self).__init__(name)

    def applies(self, replica, demand_manager): # override
        all_blocks = set([b.name for b in replica.dataset.blocks])
        locked_blocks = set(demand_manager.get_demand(replica.dataset).locked_blocks)

        intersection = all_blocks & locked_blocks
        reason = 'Blocks locked: ' + ' '.join(intersection)
        
        return len(intersection) != 0, reason


class KeepCustodial(KeepPolicy):
    """
    KEEP_OVERRIDE if the replica is custodial.
    """

    def __init__(self, name = 'KeepCustodial'):
        super(self.__class__, self).__init__(name)

    def applies(self, replica, demand_manager): # override
        return replica.is_custodial, 'Replica is custodial.'


class KeepDiskOnly(KeepPolicy):
    """
    KEEP_OVERRIDE if the replica is the last copy and the dataset is not on tape. 
    """

    def __init__(self, name = 'KeepDiskOnly'):
        super(self.__class__, self).__init__(name)

    def applies(self, replica, demand_manager): # override
        return replica.is_last_copy() and not replica.dataset.on_tape, 'Replica is a last copy with no tape copy.'


class KeepTargetOccupancy(KeepPolicy):
    """
    KEEP_OVERRIDE if occupancy of the replica's site is less than a set target.
    """

    def __init__(self, threshold, name = 'KeepTargetOccupancy'):
        super(self.__class__, self).__init__(name)

        self.threshold = threshold

    def applies(self, replica, demand_manager): # override
        return replica.site.occupancy() < self.threshold, 'Site is underused.'


class DeletePartial(DeletePolicy):
    """
    DELETE if the replica is partial.
    """

    def __init__(self, name = 'DeletePartial'):
        super(self.__class__, self).__init__(name)

    def applies(self, replica, demand_manager): # override
        return replica.is_partial, 'Replica is partial.'


class DeleteOld(DeletePolicy):
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


class DeleteUnpopular(DeletePolicy):
    """
    DELETE if this is the least popular dataset at the site.
    """

    def __init__(self, name = 'DeleteUnpopular'):
        super(self.__class__, self).__init__(name)

    def applies(self, replica, demand_manager): # override
        max_site_score = max([demand_manager.get_demand(d).popularity_score for d in replica.site.datasets])

        return demand_manager.get_demand(replica.dataset).popularity_score >= max_site_score, 'Dataset is the least popular on the site.'


class DeleteInList(DeletePolicy):
    """
    DELETE if the replica is in a list of deletions.
    The list should have a site and a dataset (wildcard allowed for both) per row, separated by white spaces.
    Any line that does not match the pattern
      <site> <dataset>
    is ignored.
    """

    def __init__(self, list_path = '', name = 'DeleteInList'):
        super(self.__class__, self).__init__(name)

        self.deletion_list = []

        if list_path:
            self.load_list(list_path)

    def load_list(self, list_path):
        with open(list_path) as deletion_list:
            for line in deletion_list:
                matches = re.match('\s*([A-Z0-9_*]+)\s+(/[\w*-]+/[\w*-]+/[\w*-]+)', line.strip())
                if not matches:
                    continue

                site_pattern = matches.group(1)
                dataset_pattern = matches.group(2)

                self.deletion_list.append((site_pattern, dataset_pattern))

    def applies(self, replica, demand_manager): # override
        """
        Loop over deletion_list and return true if the replica site and dataset matches the pattern.
        """

        for site_pattern, dataset_pattern in self.deletion_list:
            if fnmatch.fnmatch(replica.site.name, site_pattern) and fnmatch.fnmatch(replica.dataset.name, dataset_pattern):
                return True, 'Pattern match: site=%s, dataset=%s' % (site_pattern, dataset_pattern)

        return False, ''


def make_stack(strategy):
    if strategy == 'TargetFraction':
        stack = [
            KeepTargetOccupancy(detox_config.keep_target.occupancy),
            KeepIncomplete(),
            KeepLocked(),
            KeepCustodial(),
            KeepDiskOnly(),
            DeletePartial(),
            DeleteOld(*detox_config.delete_old.threshold),
#            DeleteUnpopular()
        ]

    elif strategy == 'DeletionList':
        stack = [
            KeepIncomplete(),
            KeepLocked(),
            KeepCustodial(),
            KeepDiskOnly(),
            DeleteInList()
        ]

    return stack
