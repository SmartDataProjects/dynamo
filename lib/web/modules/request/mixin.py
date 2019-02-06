import re
import fnmatch
import logging

from dynamo.web.exceptions import MissingParameter, ExtraParameter, IllFormedRequest, InvalidRequest
from dynamo.web.modules._common import yesno
from dynamo.utils.interface.mysql import MySQL
import dynamo.dataformat as df

LOG = logging.getLogger(__name__)

class ParseInputMixin(object):
    def __init__(self, config):
        # Parsed and formatted HTTP queries
        self.params = {}

    def parse_input(self, request, inventory, allowed_fields, required_fields = tuple()):
        if self.input_data is not None:
            LOG.info("Input data:")
            LOG.info(self.input_data)
        else:
            LOG.info("No input data:")
        # JSON could have been uploaded
        if self.input_data is not None:
            LOG.info("Updating input:")
            request.update(self.input_data)
            LOG.info("Completed updating input.")

        # Check we have the right request fields

        input_fields = set(request.keys())
        allowed_fields = set(allowed_fields)
        excess = input_fields - allowed_fields
        if len(excess) != 0:
            raise ExtraParameter(list(excess)[0])

        for key in required_fields:
            if key not in request:
                raise MissingParameter(key)

        # Pick up the values and cast them to correct types

        for key in ['request_id', 'n']:
            if key not in request:
                continue

            try:
                self.params[key] = int(request[key])
            except ValueError:
                raise IllFormedRequest(key, request[key], hint = '%s must be an integer' % key)

        for key in ['item', 'status', 'site', 'user']:
            if key not in request:
                continue

            value = request[key]
            if type(value) is str:
                self.params[key] = value.strip().split(',')
            elif type(value) is list:
                self.params[key] = value
            else:
                raise IllFormedRequest(key, request[key], hint = '%s must be a string or a list' % key)

        for key in ['group']:
            if key not in request:
                continue

            self.params[key] = request[key]

        for key in ['all']:
            if key not in request:
                continue

            self.params[key] = yesno(request[key])

        # Check value validity
        # We check the site, group, and item names but not use their ids in the table.
        # The only reason for this would be to make the registry not dependent on specific inventory store technology.

        if 'item' in self.params:
            for item in self.params['item']:
                if item in inventory.datasets:
                    # OK this is a known dataset
                    continue
    
                try:
                    dataset_name, block_name = df.Block.from_full_name(item)
                except df.ObjectError:
                    raise InvalidRequest('Invalid item name %s' % item)
    
                try:
                    inventory.datasets[dataset_name].find_block(block_name, must_find = True)
                except:
                    raise InvalidRequest('Invalid block name %s' % item)

        if 'site' in self.params:
            self.params['site_orig'] = []

            for site in list(self.params['site']):
                self.params['site_orig'].append(site)

                # Wildcard allowed
                if '*' in site or '?' in site or '[' in site:
                    self.params['site'].remove(site)
                    pattern = re.compile(fnmatch.translate(site))

                    for sname in inventory.sites.iterkeys():
                        if pattern.match(sname):
                            self.params['site'].append(sname)
                else:
                    try:
                        inventory.sites[site]
                    except KeyError:
                        raise InvalidRequest('Invalid site name %s' % site)

            if len(self.params['site']) == 0:
                self.params.pop('site')

        if 'group' in self.params:
            try:
                inventory.groups[self.params['group']]
            except KeyError:
                raise InvalidRequest('Invalid group name %s' % self.params['group'])

        if 'status' in self.params:
            for status in self.params['status']:
                if status not in ('new', 'activated', 'completed', 'rejected', 'cancelled'):
                    raise InvalidRequest('Invalid status value %s' % status)

        if 'cache' in request:
            self.params['cache'] = True

        LOG.info("Printing all request parameterssss")
        LOG.info(self.params)

    def make_constraints(self, by_id = False):
        constraints = {}
        if 'request_id' in self.params:
            constraints['request_id'] = self.params['request_id']

        if not by_id:
            if 'status' in self.params:
                constraints['statuses'] = self.params['status']
    
            if 'user' in self.params:
                constraints['users'] = self.params['user']
    
            if 'item' in self.params:
                constraints['items'] = self.params['item']

            if 'site' in self.params:
                constraints['sites'] = self.params['site']

        return constraints
