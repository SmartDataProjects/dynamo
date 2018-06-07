import re
import math
import logging

from dynamo.web.modules._base import WebModule
from dynamo.web.modules._common import yesno
import dynamo.web.exceptions as exceptions

LOG = logging.getLogger(__name__)

def campaign_name(dataset):
    sd = dataset.name[dataset.name.find('/', 1) + 1:dataset.name.rfind('/')]
    return sd[:sd.find('-')]

def filter_and_categorize(request, inventory, counts_only = False):
    # return {category: [(dataset_replica, [block_replica])]} or {category: [(dataset, replication)]} that match the filter

    try:
        campaign = request['campaign'].strip()
    except:
        campaign_pattern = None
    else:
        if campaign:
            campaign_pattern = re.compile(campaign)
        else:
            campaign_pattern = None

    try:
        tier = request['data_tier'].strip()
    except:
        tier_pattern = None
    else:
        if tier:
            tier_pattern = re.compile(tier)
        else:
            tier_pattern = None

    try:
        dataset = request['dataset'].strip()
    except:
        dataset_pattern = None
    else:
        if dataset:
            dataset_pattern = re.compile(dataset)
        else:
            dataset_pattern = None

    try:
        site = request['site'].strip()
    except:
        site_pattern = None
    else:
        if site:
            site_pattern = re.compile(site)
        else:
            site_pattern = None

    groups = None
    group_names = request['group[]']
    if group_names is not None:
        if type(group_names) is str:
            group_names = [group_names]

        if len(group_names) != 0:
            groups = set()

            for group_name in group_names:
                if group_name == 'None':
                    group_name = None

                try:
                    groups.add(inventory.groups[group_name])
                except KeyError:
                    pass

    try:
        list_by = request['categories'].strip()
    except:
        list_by = 'campaigns'

    product = {}

    for dataset in inventory.datasets.itervalues():
        if dataset_pattern is not None and not dataset_pattern.match(dataset.name):
            continue

        tier = dataset.name[dataset.name.rfind('/') + 1:]
    
        if tier_pattern is not None and not tier_pattern.match(tier):
            continue

        campaign = campaign_name(dataset)
    
        if campaign_pattern is not None and not campaign_pattern.match(campaign):
            continue

        replica_list = []
    
        for replica in dataset.replicas:
            if site_pattern is not None and not site_pattern.match(replica.site.name):
                continue
    
            br_list = []
    
            for block_replica in replica.block_replicas:
                if groups is None or block_replica.group in groups:
                    br_list.append(block_replica)

            if len(br_list) == 0:
                continue

            replica_list.append((replica, br_list))

        if list_by == 'campaigns':
            try:
                category_data = product[campaign]
            except KeyError:
                category_data = product[campaign] = []

            if counts_only:
                category_data.append((dataset, len(replica_list)))
            else:
                category_data.extend(replica_list)

        elif list_by == 'dataTiers':
            try:
                category_data = product[tier]
            except KeyError:
                category_data = product[tier] = []

            if counts_only:
                category_data.append((dataset, len(replica_list)))
            else:
                category_data.extend(replica_list)

        elif list_by == 'datasets':
            if counts_only:
                product[dataset.name] = [(dataset, len(replica_list))]
            else:
                product[dataset.name] = replica_list

        elif list_by == 'sites':
            for replica, br_list in replica_list:
                try:
                    category_data = product[replica.site.name]
                except KeyError:
                    category_data = product[replica.site.name] = []

                if counts_only:
                    category_data.append((dataset, 1))
                else:
                    category_data.append((replica, br_list))

        elif list_by == 'groups':
            counts = {}
            for replica, br_list in replica_list:
                by_group = {}
                for br in br_list:
                    try:
                        by_group[br.group].append(br)
                    except KeyError:
                        by_group[br.group] = [br]
                
                for group, brs in by_group.items():
                    try:
                        category_data = product[group.name]
                    except KeyError:
                        category_data = product[group.name] = []

                    if counts_only:
                        try:
                            counts[group] += 1
                        except KeyError:
                            counts[group] = 1
                    else:
                        category_data.append((replica, brs))

            if counts_only:
                for group, count in counts.items():
                    product[group.name].append((dataset, count))

    return product


class TotalSizeListing(WebModule):
    def run(self, caller, request, inventory):
        """
        @return {'dataType': 'size', 'content': [{key: key_name, size: size in TB}]}
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

        return {'dataType': 'size', 'content': content}


class ReplicationFactorListing(WebModule):
    def run(self, caller, request, inventory):
        """
        @return {'dataType': 'replication', 'content': [{key: key_name, mean: mean rep factor, rms: rms rep factor}]}
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

        return {'dataType': 'replication', 'content': content}


class SiteUsageListing(WebModule):
    def run(self, caller, request, inventory):
        """
        @return {'dataType': 'usage', 'content': [{'site': site_name, 'usage': [{key: key_name, size: size}]}]}
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

        return {'dataType': 'usage', 'content': content, 'keys': sorted(all_replicas.keys())}

export_data = {
    'stats/size': TotalSizeListing,
    'stats/replication': ReplicationFactorListing,
    'stats/usage': SiteUsageListing
}
