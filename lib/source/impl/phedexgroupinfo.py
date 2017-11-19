"""
GroupInfoSource for PhEDEx.
"""

import logging
import fnmatch

from source.groupinfo import GroupInfoSource
from common.interface.phedex import PhEDEx
from dataformat import Dataset, Block

LOG = logging.getLogger(__name__)

class PhEDExGroupInfoSource(GroupInfoSource):
    def __init__(self, config):
        GroupInfoSource.__init__(self, config)

        self._phedex = PhEDEx()

    def get_group(self, name): #override
        if self.exclude is not None:
            for pattern in self.exclude:
                if fnmatch.fnmatch(entry['name'], pattern):
                    LOG.info('get_group(%s)  %s is excluded by configuration', name, name)
                    return None

        LOG.info('get_group(%s)  Fetching info on group %s', name, name)

        result = self._phedex.call('groups', ['group=' + name])
        if len(result) == 0:
            return None

        group = Group(name)

        if name in self.dataset_level_groups:
            group.olevel = Dataset
        else:
            group.olevel = Group

        return group

    def get_group_list(self): #override
        LOG.info('get_group_list  Fetching the list of groups from PhEDEx')

        group_list = []

        for entry in self._phedex.call('groups'):
            if entry['name'] in self.dataset_level_groups:
                olevel = Dataset
            else:
                olevel = Block

            group_list.append(Group(entry['name'], olevel = olevel))

        return group_list
