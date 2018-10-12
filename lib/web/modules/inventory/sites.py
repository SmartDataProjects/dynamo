import fnmatch
import re

from dynamo.web.modules._base import WebModule
from dynamo.dataformat import Site

class ListSites(WebModule):
    """
    Simple site listing.
    """

    def run(self, caller, request, inventory):
        sites = set()
    
        # collect information from the inventory and registry according to the requests
        if 'site' in request:
            if type(request['site']) is list:
                match_names = request['site']
            else:
                match_names = request['site'].split(',')

            for match_name in match_names:
                if '*' in match_name:
                    pattern = re.compile(fnmatch.translate(match_name))
                    for name in inventory.sites.iterkeys():
                        if pattern.match(name):
                            sites.add(inventory.sites[name])
                    
                else:
                    try:
                        sites.add(inventory.sites[match_name])
                    except KeyError:
                        pass

        else:
            sites.update(inventory.sites.itervalues())

        partitions = set()

        if 'partition' in request:
            if type(request['partition']) is list:
                match_names = request['partition']
            else:
                match_names = request['partition'].split(',')

            for match_name in match_names:
                if '*' in match_name:
                    pattern = re.compile(fnmatch.translate(match_name))
                    for name in inventory.partitions.iterkeys():
                        if pattern.match(name):
                            partitions.add(inventory.partitions[name])
                    
                else:
                    try:
                        partitions.add(inventory.partitions[match_name])
                    except KeyError:
                        pass

        else:
            partitions.update(inventory.partitions.itervalues())

        response = []

        for site in sorted(sites, key = lambda s: s.name):
            data = {
                'name': site.name,
                'host': site.host,
                'storage_type': Site.storage_type_name(site.storage_type),
                'status': Site.status_name(site.status),
                'partitions': []
            }

            total_quota = 0.
            total_used = 0.
            total_projected = 0.
            
            for partition in sorted(inventory.partitions.itervalues(), key = lambda p: p.name):
                sp = site.partitions[partition]
                quota = sp.quota

                if partition.subpartitions is None:
                    part_type = 'basic'
                else:
                    part_type = 'composite'

                used = sp.occupancy_fraction() * quota
                projected = sp.occupancy_fraction(physical = False) * quota

                if part_type == 'basic' and quota > 0.:
                    total_quota += quota
                    total_used += used
                    total_projected += projected

                if partition in partitions:
                    data['partitions'].append({
                        'name': partition.name,
                        'type': part_type,
                        'quota': quota * 1.e-12,
                        'usage': used * 1.e-12,
                        'projected_usage': projected * 1.e-12
                    })

            data['total_quota'] = total_quota * 1.e-12
            data['total_usage'] = total_used * 1.e-12
            data['total_projected_usage'] = total_projected * 1.e-12

            response.append(data)
    
        # return any JSONizable python object (maybe should be limited to a list)
        return response


# exported to __init__.py
export_data = {
    'sites': ListSites
}
