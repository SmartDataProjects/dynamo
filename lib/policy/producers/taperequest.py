import re
import collections
import logging

from dynamo.utils.interface.phedex import PhEDEx
from dynamo.dataformat import Block, Site

LOG = logging.getLogger(__name__)

class TapeCopyRequested(object):
    """
    Check for pending tape transfer requests.
    Sets one attr:
      tape_copy_requested
    """

    produces = ['tape_copy_requested']

    def __init__(self, config):
        self._phedex = PhEDEx(config.phedex)

    def load(self, inventory):
        for site in inventory.sites.itervalues():
            if site.storage_type != Site.TYPE_MSS:
                continue

            requests = phedex.make_request('transferrequests', ['node=' + site.name, 'approval=pending'])
            for request in requests:
                for dest in request['destinations']['node']:
                    if dest['name'] != site.name:
                        continue

                    if 'decided_by' in dest:
                        break

                    for dataset_entry in request['data']['dbs']['dataset']:
                        try:
                            dataset = inventory.datasets[dataset_entry['name']]
                        except KeyError:
                            continue

                        dataset.attr['tape_copy_requested'] = True

                    for block_entry in request['data']['dbs']['block']:
                        dataset_name, block_name = Block.from_full_name(block_entry['name'])
                        try:
                            dataset = inventory.datasets[dataset_name]
                        except KeyError:
                            continue

                        # just label the entire dataset
                        dataset.attr['tape_copy_requested'] = True
