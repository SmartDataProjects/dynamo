import logging
import fnmatch

from dynamo.source.groupinfo import GroupInfoSource
from dynamo.utils.interface.phedex import PhEDEx
from dynamo.dataformat import Group, Dataset, Block

LOG = logging.getLogger(__name__)

class PhEDExGroupInfoSource(GroupInfoSource):
    """GroupInfoSource using PhEDEx."""

    def __init__(self, config):
        GroupInfoSource.__init__(self, config)

        self._phedex = PhEDEx(config.phedex)

    def get_group(self, name): #override
        if self.include is not None:
            matched = False
            for pattern in self.include:
                if fnmatch.fnmatch(name, pattern):
                    matched = True
                    break

            if not matched:
                LOG.info('get_group(%s)  %s is not included by configuration', name, name)
                return None

        if self.exclude is not None:
            for pattern in self.exclude:
                if fnmatch.fnmatch(name, pattern):
                    LOG.info('get_group(%s)  %s is excluded by configuration', name, name)
                    return None

        LOG.info('get_group(%s)  Fetching info on group %s', name, name)

        result = self._phedex.make_request('groups', ['group=' + name])
        if len(result) == 0:
            return None

        group = Group(name)

        if name in self.dataset_level_groups:
            group.olevel = Dataset
        else:
            group.olevel = Block

        return group

    def get_group_list(self): #override
        LOG.info('get_group_list  Fetching the list of groups from PhEDEx')
        LOG.debug('Groups with dataset-level ownership: %s', str(self.dataset_level_groups))

        group_list = []

        for entry in self._phedex.make_request('groups'):
            if self.include is not None:
                matched = False
                for pattern in self.include:
                    if fnmatch.fnmatch(entry['name'], pattern):
                        matched = True
                        break
    
                if not matched:
                    continue
    
            if self.exclude is not None:
                matched = False
                for pattern in self.exclude:
                    if fnmatch.fnmatch(entry['name'], pattern):
                        matched = True
                        break

                if matched:
                    continue

            if entry['name'] in self.dataset_level_groups:
                olevel = Dataset
            else:
                olevel = Block

            group_list.append(Group(entry['name'], olevel = olevel))

        return group_list
