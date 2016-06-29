from detox.policy import Policy
from common.dataformat import Site

class Protect(object):
    """
    Base class for callable object returning a PROTECT decision.
    """
    def __call__(self, replica, demand_manager):
        reason = self._do_call(replica, demand_manager)
        if reason is not None:
            return replica, Policy.DEC_PROTECT, reason


class Delete(object):
    """
    Base class for callable object returning a DELETE decision.
    """
    def __call__(self, replica, demand_manager):
        reason = self._do_call(replica, demand_manager)
        if reason is not None:
            return replica, Policy.DEC_DELETE, reason


class ProtectIncomplete(Protect):
    """
    PROTECT if the replica is not complete.
    """
    def _do_call(self, replica, demand_manager):
        if not replica.is_complete:
            return 'Replica is not complete.'

protect_incomplete = ProtectIncomplete()


class ProtectLocked(Protect):
    """
    PROTECT if any block of the dataset is locked.
    """
    def _do_call(self, replica, demand_manager):
        all_blocks = set([b.real_name() for b in replica.dataset.blocks])
        locked_blocks = set(demand_manager.dataset_demands[replica.dataset].locked_blocks)
    
        intersection = all_blocks & locked_blocks
        reason = 'Blocks locked: ' + ' '.join(intersection)
    
        if len(intersection) != 0:
            return reason

protect_locked = ProtectLocked()


class ProtectCustodial(Protect):
    """
    PROTECT if the replica is custodial.
    """
    def _do_call(self, replica, demand_manager):
        if replica.is_custodial:
            return 'Replica is custodial.'

protect_custodial = ProtectCustodial()


class ProtectDiskOnly(Protect):
    """
    PROTECT if the dataset is not on tape. 
    """
    def _do_call(self, replica, demand_manager):
        if not replica.dataset.on_tape:
            return 'Dataset has no complete tape copy.'

protect_diskonly = ProtectDiskOnly()


class ProtectNonreadySite(Protect):
    """
    PROTECT if the site is not ready.
    """
    def _do_call(self, replica, demand_manager):
        if replica.site.status != Site.STAT_READY:
            return 'Site is not in ready state.'

protect_nonready_site = ProtectNonreadySite()


class ProtectMinimumCopies(Protect):
    """
    PROTECT if the replica has fewer than or equal to minimum number of copies.
    """
    def _do_call(self, replica, demand_manager):
        required_copies = demand_manager.dataset_demands[replica.dataset].required_copies
        if len(replica.dataset.replicas) <= required_copies:
            return 'Dataset has <= ' + str(required_copies) + ' copies.'

protect_minimum_copies = ProtectMinimumCopies()


class ProtectNotOwnedBy(Protect):
    """
    PROTECT if the replica is not fully owned by a group.
    """
    
    def __init__(self, group_name):
        self.group_name = group_name

    def _do_call(self, replica, demand_manager):
        if replica.group is None or replica.group.name != self.group_name:
            return 'Not all parts of replica is owned by ' + self.group_name


class DeletePartial(Delete):
    """
    DELETE if the replica is partial.
    """
    def _do_call(self, replica, demand_manager):
        if replica.is_partial:
            return 'Replica is partial.'

delete_partial = DeletePartial()


class DeleteOlderThan(Delete):
    """
    DELETE if the replica is not accessed for more than a set time.
    """

    def __init__(self, threshold, unit):
        self.threshold_text = '%.1f%s' % (threshold, unit)

        if unit == 'y':
            threshold *= 365.
        if unit == 'y' or unit == 'd':
            threshold *= 24.
        if unit == 'y' or unit == 'd' or unit == 'h':
            threshold *= 3600.

        cutoff_timestamp = time.time() - threshold
        cutoff_datetime = datetime.datetime.utcfromtimestamp(cutoff_timestamp)
        self.cutoff = cutoff_datetime.date()

    def _do_call(self, replica, demand_manager):
        # the dataset was updated after the cutoff -> don't delete
        last_update = datetime.datetime.utcfromtimestamp(replica.dataset.last_update).date()
        if last_update > self.cutoff:
            return None

        # no accesses recorded ever -> delete
        if len(replica.accesses) == 0:
            return 'Replica was created on', last_update.strftime('%Y-%m-%d'), 'but is never accessed.'

        for acc_type, records in replica.accesses.items(): # remote and local
            if len(records) == 0:
                continue

            last_acc_date = max(records.keys()) # datetime.date object set to UTC

            if last_acc_date > self.cutoff:
                return None
            
        return 'Last access is older than ' + self.threshold_text + '.'


class ActionList(object):
    """
    Take decision from a list of policies.
    The list should have a decision, a site, and a dataset (wildcard allowed for both) per row, separated by white spaces.
    Any line that does not match the pattern
      (Keep|Delete) <site> <dataset>
    is ignored.
    """

    def __init__(self, list_path = ''):
        self.res = [] # (action, site_re, dataset_re)
        self.patterns = [] # (action_str, site_pattern, dataset_pattern)

        if list_path:
            self.load_list(list_path)

    def add_action(self, action_str, site_pattern, dataset_pattern):
        site_re = re.compile(fnmatch.translate(site_pattern))
        dataset_re = re.compile(fnmatch.translate(dataset_pattern))

        if action_str == 'Keep':
            action = Policy.DEC_PROTECT
        else:
            action = Policy.DEC_DELETE

        self.res.append((action, site_re, dataset_re))
        self.patterns.append((action_str, site_pattern, dataset_pattern))

    def load_list(self, list_path):
        with open(list_path) as deletion_list:
            for line in deletion_list:
                matches = re.match('\s*(Keep|Delete)\s+([A-Za-z0-9_*]+)\s+(/[\w*-]+/[\w*-]+/[\w*-]+)', line.strip())
                if not matches:
                    continue

                action_str = matches.group(1)
                site_pattern = matches.group(2)
                dataset_pattern = matches.group(3)

                self.add_action(action_str, site_pattern, dataset_pattern)

    def load_lists(self, list_paths):
        for list_path in list_paths:
            self.load_list(list_path)

    def __call__(self, replica, demand_manager):
        """
        Loop over the patterns list and make an entry in self.actions if the pattern matches.
        """

        matches = []
        for iL, (action, site_re, dataset_re) in enumerate(self.res):
            if site_re.match(replica.site.name) and dataset_re.match(replica.dataset.name):
                action = action
                matches.append(self.patterns[iL])

        if len(matches) != 0:
            return replica, action, 'Pattern match: (action, site, dataset) = [%s]' % (','.join(['(%s, %s, %s)' % match for match in matches]))
    

def make_stack(strategy):
    # return a *function* that returns the selected stack

    if strategy == 'Routine':
        def stackgen(*arg, **kwd):
            stack = [
                protect_nonready_site
                protect_incomplete,
                protect_diskonly,
                delete_old,
                delete_partial,
                protect_minimum_copies
            ]

            return stack, Policy.DEC_DELETE

    elif strategy == 'List':
        # stackgen([files]) -> List stack with files loaded into ActionList
        def stackgen(*arg, **kwd):
            stack = [
                protect_incomplete,
                protect_diskonly,
                ActionList()
            ]

            if type(arg[0]) is list:
                stack[-1].load_lists(arg[0])
            else:
                stack[-1].load_list(arg[0])

            return stack, Policy.DEC_PROTECT

    return stackgen
