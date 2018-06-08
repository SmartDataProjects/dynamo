import re
import math
import json
import collections

from dynamo.web.modules._base import WebModule
from dynamo.web.modules._html import HTMLMixin
from dynamo.web.modules._common import yesno
import dynamo.web.exceptions as exceptions
from dynamo.dataformat import Dataset, Site, Group

class InventoryStatCategories(object):
    categories = collections.OrderedDict([
        ('data_type', ('Dataset type', Dataset, lambda d: d.data_type)),
        ('dataset_status', ('Dataset status', Dataset, lambda d: d.status)),
        ('dataset_software_version', ('Dataset software version', Dataset, lambda d: d.software_version)),
        ('dataset', ('Dataset name', Dataset, lambda d: d.name)),
        ('site', ('Site name', Site, lambda s: s.name)),
        ('site_status', ('Site status', Site, lambda s: s.name)),
        ('group', ('Group name', Group, lambda g: g.name))
    ])

def passes_constraints(item, constraints):
    if len(constraints) == 0:
        return True

    for category, pattern in constraints.iteritems():
        valuemap = InventoryStatCategories.categories[category][2]
        value = keymap(item)

        if type(pattern) is list:
            # ORed list
            match = True
            for pat in pattern:
                if pat is None and value is None:
                    break
                elif pat.match(value):
                    break
            else:
                # no pattern matched
                return False
                
        elif pattern is None and value is not None:
            return False

        elif not pattern.match(value):
            return False

    return True


def filter_and_categorize(request, inventory, counts_only = False):
    # return {category: [(dataset_replica, [block_replica])]} or {category: [(dataset, replication)]} that match the filter

    # constraints
    dataset_constraints = {}
    site_constraints = {}
    group_constraints = {}

    for category, (_, target, _) in InventoryStatCategories.categories.keys():
        if target is Dataset:
            constraints = dataset_constraints
        elif target is Site:
            constraints = site_constraints
        elif target is Group:
            constraints = group_constraints

        try:
            const_str = request[category].strip()
        except KeyError:
            const_str = None

        if const_str:
            if const_str == 'None':
                constraints[category] = None
            else:
                constraints[category] = re.compile(const_str)
            break

        try:
            const_list = request[category + '[]']
            if type(const_list) is str: # if there is only one item given
                const_list = const_list.strip()
        except KeyError:
            const_list = ''

        if len(const_list) != 0:
            constraints[category] = []

            if type(const_list) is list:
                for const_str in const_list:
                    if const_str == 'None':
                        constraints[category].append(None)
                    else:
                        constraints[category].append(re.compile(const_str))
    
            elif type(const_list) is str:
                if const_list == 'None':
                    constraints[category].append(None)
                else:
                    constraints[category].append(re.compile(const_list))

    try:
        list_by = request['list_by'].strip()
    except:
        list_by = next(cat for cat in InventoryStatCategories.categories.iterkeys())

    product = {}

    matching_sites = set()
    for site in inventory.sites.itervalues():
        if passes_constraints(site, site_constraints):
            matching_sites.add(site)

    matching_groups = set()
    for group in inventory.groups.itervalues():
        if passes_constraints(group, group_constraints):
            matching_groups.add(group)
    
    for dataset in inventory.datasets.itervalues():
        if not passes_constraints(dataset, dataset_constraints):
            continue

        replica_list = []
    
        for replica in dataset.replicas:
            if replica.site not in matching_sites:
                continue
    
            br_list = []
    
            for block_replica in replica.block_replicas:
                if block_replica.group in matching_groups:
                    br_list.append(block_replica)

            if len(br_list) == 0:
                continue

            replica_list.append((replica, br_list))

        _, target, keymap = InventoryStatCategories.categories[list_by]

        if target is Dataset:
            key = keymap(dataset)

            try:
                category_data = product[key]
            except KeyError:
                category_data = product[key] = []

            if counts_only:
                category_data.append((dataset, len(replica_list)))
            else:
                category_data.extend(replica_list)

        elif target is Site:
            for replica, br_list in replica_list:
                key = keymap(replica.site)

                try:
                    category_data = product[key]
                except KeyError:
                    category_data = product[key] = []

                if counts_only:
                    category_data.append((dataset, 1))
                else:
                    category_data.append((replica, br_list))

        elif target is Group:
            counts = {}
            for replica, br_list in replica_list:
                by_group = {}
                for br in br_list:
                    try:
                        by_group[br.group].append(br)
                    except KeyError:
                        by_group[br.group] = [br]
                
                for group, brs in by_group.items():
                    key = keymap(group)

                    try:
                        category_data = product[key]
                    except KeyError:
                        category_data = product[key] = []

                    if counts_only:
                        try:
                            counts[group] += 1
                        except KeyError:
                            counts[group] = 1
                    else:
                        category_data.append((replica, brs))

            if counts_only:
                for group, count in counts.items():
                    product[key].append((dataset, count))

    return product


class TotalSizeListing(WebModule):
    def run(self, caller, request, inventory):
        """
        @return {'statistic': 'size', 'content': [{key: key_name, size: size in TB}]}
        """

        if yesno(request, 'physical'):
            get_size = lambda bl: sum(br.size for br in bl)
        else:
            get_size = lambda bl: sum(br.block.size for br in bl)

        all_replicas = filter_and_categorize(request, inventory)

        content = []

        for category, replicas in all_replicas.iteritems():
            size = 0
            for dataset_replica, block_replicas in replicas:
                size += get_size(block_replicas)

            content.append({'key': category, 'size': size * 1.e-12})

        content.sort(key = lambda x: x['size'], reverse = True)

        return {'statistic': 'size', 'content': content}


class ReplicationFactorListing(WebModule):
    def run(self, caller, request, inventory):
        """
        @return {'statistic': 'replication', 'content': [{key: key_name, mean: mean rep factor, rms: rms rep factor}]}
        """

        dataset_counts = filter_and_categorize(request, inventory, counts_only = True)

        content = []

        for category, datasets in dataset_counts.iteritems():
            sumw = 0.
            sumw2 = 0.
            n = 0
            for dataset, count in datasets:
                sumw += count
                sumw2 += count * count
                n += 1

            mean = sumw / n
            rms = math.sqrt(sumw2 / n - mean * mean)
            content.append({'key': category, 'mean': mean, 'rms': rms})

        content.sort(key = lambda x: x['mean'], reverse = True)

        return {'statistic': 'replication', 'content': content}


class SiteUsageListing(WebModule):
    def run(self, caller, request, inventory):
        """
        @return {'statistic': 'usage', 'content': [{'site': site_name, 'usage': [{key: key_name, size: size}]}]}
        """

        if yesno(request, 'physical', True):
            get_size = lambda bl: sum(br.size for br in bl)
        else:
            get_size = lambda bl: sum(br.block.size for br in bl)

        all_replicas = filter_and_categorize(request, inventory)

        by_site = {} # {site: {category: replicas}}

        for category, replicas in all_replicas.iteritems():
            for dataset_replica, block_replicas in replicas:
                try:
                    site_content = by_site[dataset_replica.site]
                except KeyError:
                    site_content = by_site[dataset_replica.site] = {}

                try:
                    site_content[category].append((dataset_replica, block_replicas))
                except KeyError:
                    site_content[category] = [(dataset_replica, block_replicas)]

        content = []

        for site, site_replicas in by_site.iteritems():
            site_content = []

            for category, replicas in site_replicas.iteritems():
                size = 0
                for dataset_replica, block_replicas in replicas:
                    size += get_size(block_replicas)

                site_content.append({'key': category, 'size': size * 1.e-12})

            site_content.sort(key = lambda x: x['size'], reverse = True)

            content.append({'site': site.name, 'usage': site_content})

        content.sort(key = lambda x: x['site'])

        return {'statistic': 'usage', 'content': content, 'keys': sorted(all_replicas.keys())}


class InventoryStats(WebModule, HTMLMixin):
    """
    The original inventory monitor showing various inventory statistics.
    """

    def __init__(self, config):
        WebModule.__init__(self, config) 
        HTMLMixin.__init__(self, 'Dynamo inventory statistics', config.inventory.stats.body_html)

        self.stylesheets = ['/css/inventory/stats.css']
        self.scripts = ['/js/utils.js', '/js/inventory/stats.js']

        self.default_constraints = config.inventory.monitor.default_constraints

    def run(self, caller, request, inventory):
        # Parse GET and POST requests and set the defaults
        try:
            statistic = request['statistic'].strip()
        except:
            statistic = 'size'

        try:
            list_by = request['list_by'].strip()
        except:
            list_by = next(cat for cat in InventoryStatCategories.categories.iterkeys())

        constraints = {}
        for key in InventoryStatCategories.categories.iterkeys():
            try:
                constraint = request[key]
            except KeyError:
                try:
                    constraint = request[key + '[]']
                except KeyError:
                    continue

            if type(constraint) is str:
                constraint = constraint.strip()
                if ',' in constraint:
                    constraint = constraint.split(',')

            constraints[key] = constraint

        if len(constraints) == 0:
            constraints = self.default_constraints

        # HTML formatting

        self.header_script = '$(document).ready(function() { initPage(\'%s\', \'%s\', %s); });' % (statistic, categories, json.dumps(constraints))

        repl = {}

        categories_html = ''
        constraint_types_html = ''
        constraint_inputs_html = ''
        for name, (title, _, _) in InventoryStatCategories.categories.iteritems():
            categories_html += '                <option value="%s">%s</option>\n' % (name, title)
            constraint_types_html += '              <div class="constraintType">%s</div>\n' % title
            if name != 'group':
                constraint_inputs_html += '              <div class="constraintInput"> = <input class="constraint" type="text" id="%s" name="%s"></div>' % (name, name)

        repl['CATEGORIES'] = categories_html
        repl['CONSTRAINT_TYPES'] = constraint_types_html
        repl['CONSTRAINT_INPUTS'] = constraint_inputs_html

        if yesno(request, 'physical', True):
            repl['PHYSICAL_CHECKED'] = ' checked="checked"'
            repl['PROJECTED_CHECKED'] = ''
        else:
            repl['PHYSICAL_CHECKED'] = ''
            repl['PROJECTED_CHECKED'] = ' checked="checked"'

        return self.form_html(repl)


export_data = {
    'stats/size': TotalSizeListing,
    'stats/replication': ReplicationFactorListing,
    'stats/usage': SiteUsageListing
}

export_web = {
    'stats': InventoryStats
}
