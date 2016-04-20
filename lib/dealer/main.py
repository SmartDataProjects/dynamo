import time
import datetime
import collections
import logging

from common.dataformat import Dataset, DatasetReplica
from common.interface.mysql import MySQL
import common.configuration as config
import dealer.configuration as dealer_config

logger = logging.getLogger(__name__)

class Dealer(object):

    def __init__(self, inventory, transaction, demand):
        self.inventory_manager = inventory
        self.transaction_manager = transaction
        self.demand_manager = demand

        self.copy_message = 'DynaMO -- Automatic Replication Request.'

        self._history = MySQL(**config.history.db_params)

    def run(self):
        """
        1. Update the inventory if necessary.
        2. Update popularity.
        3. Create pairs of (new replica, source) representing copy operations that should take place.
        4. Execute copy.
        """
        
        logger.info('Dealer run starting at %s', time.strftime('%Y-%m-%d %H:%M:%S'))

        if time.time() - self.inventory_manager.store.last_update > config.inventory.refresh_min:
            logger.info('Inventory was last updated at %s. Reloading content from remote sources.', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.inventory_manager.store.last_update)))
            # inventory is stale -> update
            self.inventory_manager.update()

        self.demand_manager.load(self.inventory_manager)

#        utcnow = datetime.datetime.utcnow()
#        utcmidnight = datetime.datetime(utcnow.year, utcnow.month, utcnow.day)
#        if (utcnow - utcmidnight).seconds > self.demand_manager.time_today + dealer_config.demand_refresh_interval:
#            self.demand_manager.update_accesses(self.inventory_manager)

        self.demand_manager.update(self.inventory_manager)

#        copy_list = self.determine_copies_by_accesses()
        copy_list = self.determine_copies_by_requests()

        self.commit_copies(copy_list)

    def determine_copies_by_accesses(self):
        """
        Simplest algorithm:
        1. Compute time- and slot- normalized CPU hour (site occupancy fraction) for each dataset replica.
        2. Sum up occupancy for each site.
        3. At each site, datasets with occupancy fraction > threshold are placed in the list of copy candidates.
        #5. Sum up occupancy fractions for each dataset.
        #6. Datasets whose global occupancy fractions are increasing are placed in the list of copy candidates.
        """

        sites = self.inventory_manager.sites.values()

        copy_list = dict([(site, []) for site in sites]) # site -> [(new_replica, origin)]

        now = time.time() # UNIX timestamp of now
        utc = time.gmtime(now) # struct_time in UTC
        utctoday = datetime.date(utc.tm_year, utc.tm_mon, utc.tm_mday)
        last_three_days = [utctoday - datetime.timedelta(2), utctoday - datetime.timedelta(1), utctoday]

        group = self.inventory_manager.groups[dealer_config.operating_group]

        dataset_cpus = collections.defaultdict(list) # dataset -> [(ncpu, site)]
        site_cpu_occupancies = {}
        site_storage = {}

        for site in sites:
            logger.info('Inspecting replicas at %s', site.name)

            site_occupancy = 0.
                
            for replica in site.dataset_replicas:
                if replica.group != group:
                    continue

                # TODO THINK ABOUT THIS
                if replica.dataset.name.endswith('GEN-SIM-RAW'):
                    continue

                # skip non-complete datasets
                if replica.dataset.status != Dataset.STAT_VALID:
                    continue

                # skip datasets size > 10 TB
                if replica.dataset.size * 1.e-12 > 10.:
                    continue

                accesses = replica.accesses[DatasetReplica.ACC_LOCAL]

                dataset_ncpu = 0.
                for date in last_three_days:
                    try:
                        access = accesses[date]
                    except KeyError:
                        continue

                    if date == utctoday:
                        time_norm = self.demand_manager.time_today
                    else:
                        time_norm = 24. * 3600.

                    ncpu = access.cputime * 3600. / time_norm

                    dataset_ncpu += ncpu
                    site_occupancy += ncpu / site.cpu

                if dataset_ncpu != 0.:
                    logger.info('%s has time-normalized CPU usage of %f', replica.dataset.name, dataset_ncpu)

                dataset_cpus[replica.dataset].append((dataset_ncpu, site))

            site_cpu_occupancies[site] = site_occupancy

            site_storage[site] = site.storage_occupancy(group)

            logger.info('%s occupancy: %f (CPU) %f (storage)', site.name, site_cpu_occupancies[site], site_storage[site])

        popular_datasets = [] # (dataset, total_ncpu, total_site_capacity)

        for dataset, usage_list in dataset_cpus.items():
            total_ncpu = sum(n for n, site in usage_list)

            if total_ncpu / dataset.num_files < dealer_config.popularity_threshold:
                continue

            total_site_capacity = sum(site.cpu for n, site in usage_list)

            if total_ncpu / total_site_capacity < dealer_config.occupancy_fraction_threshold:
                continue

            logger.info('%s is popular and needs more resource.', dataset.name)

            if len(dataset.replicas) > dealer_config.max_replicas:
                logger.warning('%s has too many replicas already. Not copying.', dataset.name)
                continue

            popular_datasets.append((dataset, total_ncpu, total_site_capacity))

        popular_datasets.sort(key = lambda (d, n, c): n / d.num_files, reverse = True)

        site_cpu_occupancies_before = dict(site_cpu_occupancies)
        site_storage_before = dict(site_storage)
        copy_volumes = dict([(site, 0.) for site in sites])

        for dataset, total_ncpu, total_site_capacity in popular_datasets:

            global_stop = False

            while total_ncpu / dataset.num_files > len(dataset.replicas) * dealer_config.reference_cpu_per_file:
                sorted_sites = sorted(site_cpu_occupancies.items(), key = lambda (s, o): o) #sorted from emptiest to busiest
    
                try:
                    destination_site = next(dest for dest, occ in sorted_sites if \
                        occ != 0. and \
                        site_storage[dest] < config.target_site_occupancy and \
                        site_storage[dest] + dataset.size * 1.e-12 / dest.group_quota[group] < 1. and \
                        copy_volumes[dest] < dealer_config.max_copy_volume and \
                        dest.find_dataset_replica(dataset) is None
                    )
                    # occ == 0 -> this site had no activities - probably not active.
                    # site is not full
                    # site will not be full
                    # copy quota not reached yet
                    # does not have the dataset replica yet

                except StopIteration:
                    logger.warning('%s has no copy destination.', dataset.name)
                    break
    
                logger.info('Copying %s to %s', dataset.name, destination_site.name)
    
                origin_site = max(usage_list, key = lambda (n, site): n)[1] # busiest site for this dataset
    
                new_replica = self.inventory_manager.add_dataset_to_site(dataset, destination_site, group)
                copy_list[destination_site].append((new_replica, origin_site))
    
                copy_volumes[destination_site] += dataset.size * 1.e-12
    
                # add the size of this replica to destination storage
                site_storage[destination_site] += dataset.size * 1.e-12 / destination_site.group_quota[group]

                # add the cpu capacity of the destination site to total
                total_site_capacity += destination_site.cpu
                
                # add the cpu usage of this replica to destination occupancy
                site_cpu_occupancies[destination_site] += total_ncpu / total_site_capacity

                if len(dataset.replicas) > dealer_config.max_replicas:
                    logger.warning('%s has reached the maximum number of replicas allowed.', dataset.name)
                    break
    
                if min(site_storage.values()) > config.target_site_occupancy:
                    logger.warning('All sites have exceeded target storage occupancy. No more copies will be made.')
                    global_stop = True
                    break
    
                if min(copy_volumes.values()) > dealer_config.max_copy_volume:
                    logger.warning('All sites have exceeded copy volume target. No more copies will be made.')
                    global_stop = True
                    break

            if global_stop:
                break

        self.write_summary_by_accesses(dataset_cpus, copy_list, site_cpu_occupancies_before, site_storage_before, site_storage)

        return copy_list

    def determine_copies_by_requests(self):
        """
        Algorithm:
        1. Compute a time-weighted sum of number of requests for the last three days.
        2. Decide the sites least-occupied by analysis activities.
        3. Copy datasets with number of requests > available replicas to empty sites.
        """
        
        sites = self.inventory_manager.sites.values()
        
        copy_list = dict([(site, []) for site in sites]) # site -> [(new_replica, origin)]

        group = self.inventory_manager.groups[dealer_config.operating_group]

        now = time.time()
        time_bins = []
        # will make this configurable
        time_bins.append((now - 3600 * 24 * 7, 0.1))
        time_bins.append((now - 3600 * 24 * 3, 0.5))
        time_bins.append((now - 3600 * 24, 0.7))
        time_bins.append((now - 3600 * 12, 1.))

        # time-weighted dataset popularity
        dataset_weights = {}
        # time-weighted, cpu-normalized number of running jobs at sites
        site_nfiles = dict([(site, 0.) for site in sites])

        for dataset in self.inventory_manager.datasets.values():
            if dataset.status != Dataset.STAT_VALID:
                continue

            if dataset.size * 1.e-12 > 10.:
                continue

            if len(dataset.replicas) > dealer_config.max_replicas:
                continue

            dataset_weight = 0.

            for request in dataset.requests:
                if request.queue_time <= time_bins[0][0]:
                    # this request is too old to consider
                    continue

                for ibin in range(len(time_bins) - 1):
                    if request.queue_time < time_bins[ibin + 1][0]:
                        weight = time_bins[ibin][1]
                        break
                else:
                    # in the last bin
                    weight = time_bins[-1][1]

                dataset_weight += weight

            if dataset_weight > 0.:
                dataset_weights[dataset] = dataset_weight

                total_cpu = sum([r.site.cpu for r in dataset.replicas])
                for replica in dataset.replicas:
                    # w * N * (site cpu / total cpu); normalized by site cpu
                    site_nfiles[replica.site] += dataset_weight * dataset.num_files / total_cpu

        site_nfiles_before = dict(site_nfiles)
        
        site_storage = dict([(site, site.storage_occupancy(group)) for site in sites])
        site_storage_before = dict(site_storage)

        copy_volumes = dict([(site, 0.) for site in sites])

        # now go through datasets sorted by weight / #replicas
        for dataset, weight in sorted(dataset_weights.items(), key = lambda (d, w): w / len(d.replicas), reverse = True):

            global_stop = False

            while weight > len(dataset.replicas) * 1.5:
                sorted_sites = sorted(site_nfiles.items(), key = lambda (s, n): n) #sorted from emptiest to busiest

                try:
                    destination_site = next(dest for dest, njob in sorted_sites if \
                        not dest.name.startswith('T1_') and \
                        site_storage[dest] < config.target_site_occupancy and \
                        site_storage[dest] + dataset.size * 1.e-12 / dest.group_quota[group] < 1. and \
                        copy_volumes[dest] < dealer_config.max_copy_volume and \
                        dest.find_dataset_replica(dataset) is None
                    )

                except StopIteration:
                    logger.warning('%s has no copy destination.', dataset.name)
                    break

                logger.info('Copying %s to %s', dataset.name, destination_site.name)

                try:
                    origin_site = max(dataset.replicas, key = lambda r: site_nfiles[r.site]).site # busiest site for this dataset
                except ValueError:
                    # dataset has 0 replicas
                    origin_site = None
    
                new_replica = self.inventory_manager.add_dataset_to_site(dataset, destination_site, group)
                copy_list[destination_site].append((new_replica, origin_site))
    
                copy_volumes[destination_site] += dataset.size * 1.e-12
    
                # add the size of this replica to destination storage
                site_storage[destination_site] += dataset.size * 1.e-12 / destination_site.group_quota[group]

                # add the cpu load to the destination site and adjust the existing sites
                total_cpu = sum([r.site.cpu for r in dataset.replicas])
                total_cpu_before = total_cpu - destination_site.cpu

                for replica in dataset.replicas:
                    if replica != new_replica:
                        site_nfiles[replica.site] -= weight * dataset.num_files / total_cpu_before

                    site_nfiles[replica.site] += weight * dataset.num_files / total_cpu

                if len(dataset.replicas) > dealer_config.max_replicas:
                    logger.warning('%s has reached the maximum number of replicas allowed.', dataset.name)
                    break
    
                if min(site_storage.values()) > config.target_site_occupancy:
                    logger.warning('All sites have exceeded target storage occupancy. No more copies will be made.')
                    global_stop = True
                    break
    
                if min(copy_volumes.values()) > dealer_config.max_copy_volume:
                    logger.warning('All sites have exceeded copy volume target. No more copies will be made.')
                    global_stop = True
                    break

            if global_stop:
                break

        self.write_summary_by_requests(dataset_weights, copy_list, site_nfiles_before, site_storage_before, site_storage)

        return copy_list

    def commit_copies(self, copy_list):
        # first make sure the list of blocks is up-to-date
        datasets = []
        for site, replica_origins in copy_list.items():
            for replica, origin in replica_origins:
                if replica.dataset not in datasets:
                    datasets.append(replica.dataset)

        self.inventory_manager.dataset_source.set_dataset_constituent_info(datasets)

        for site, replica_origins in copy_list.items():
            if len(replica_origins) == 0:
                continue

            copy_mapping = self.transaction_manager.copy.schedule_copies(replica_origins, comments = self.copy_message)
            # copy_mapping .. {operation_id: (complete, [(replica, origin)])}
    
            for operation_id, (completed, list_chunk) in copy_mapping.items():
                replicas = [r for r, o in list_chunk]
                if completed:
                    self.inventory_manager.store.add_dataset_replicas(replicas)
                    self.inventory_manager.store.set_last_update()
    
                size = sum([r.size() for r in replicas])

                self.make_history_entries(site, operation_id, completed, list_chunk, size)

    def make_history_entries(self, site, operation_id, completed, ro_list, size):
        if config.read_only:
            logger.info('dealer.make_history_entries')
            return

        self._history.insert_many('sites', ('name',), lambda (r, o): (o.name,), ro_list)
        self._history.insert_many('sites', ('name',), lambda s: (s.name,), [site])

        site_ids = dict(self._history.query('SELECT `name`, `id` FROM `sites`'))

        dataset_names = list(set(r.dataset.name for r, o in ro_list))
        
        self._history.insert_many('datasets', ('name',), lambda name: (name,), dataset_names)
        dataset_ids = dict(self._history.query('SELECT `name`, `id` FROM `datasets`'))

        self._history.query('INSERT INTO copy_history (`id`, `timestamp`, `completed`, `site_id`, `size`) VALUES (%s, NOW(), %s, %s, %s)', operation_id, completed, site_ids[site.name], size)

        self._history.insert_many('copied_replicas', ('copy_id', 'dataset_id', 'origin_site_id'), lambda (r, o): (operation_id, dataset_ids[r.dataset.name], site_ids[o.name]), ro_list)

    def write_summary_by_accesses(self, dataset_cpus, copy_list, cpu_before, storage_before, storage_after):
        html = open(dealer_config.summary_html, 'w')

        text = '''<html>
<html>
 <head>
  <title>Dealer copy decision records</title>
  <style>
body {
  font-family: Monospace;
  font-size: 120%;
}

span.menuitem {
  text-decoration: underline;
  cursor:pointer;
}

span.popular {
  color: red;
}

.collapsible {
  display: none;
}

li.vanishable {
  display: list-item;
}

ul.arrive-bullet {
  list-style-type: square;
}
  </style>
  <script type="text/javascript">
function toggleVisibility(id) {
  var list = document.getElementById(id);
  if (list.style.display == "block")
    list.style.display = "none";
  else
    list.style.display = "block";
}
function isCollapsible(obj) {
  var iC = 0;
  for (; iC != obj.classList.length; ++iC) {
    if (obj.classList[iC] == "collapsible")
      break;
  }
  return iC != obj.classList.length;
}
function isVanishable(obj) {
  var iC = 0;
  for (; iC != obj.classList.length; ++iC) {
    if (obj.classList[iC] == "vanishable")
      break;
  }
  return iC != obj.classList.length;
}
function isInList(obj, list) {
  var iC = 0;
  for (; iC != list.length; ++iC) {
    if (list[iC] == obj)
      break;
  }
  return iC != list.length;
}
function searchDataset(name) {
  if (name.length > 0 && name.length < 6)
    return;

  var listitems = document.getElementsByTagName("li");
  var listitem;
  var sublist;
  var colon;
  var list;
  var body = document.getElementsByTagName("body")[0];
  var hits = [];

  for (var iL = 0; iL != listitems.length; ++iL) {
    listitem = listitems[iL];

    if (name == "") {
      listitem.style.display = "list-item";

      list = listitem.parentNode;
      if (listitem == list.firstChild) {
        while (true) {
          if (isCollapsible(list))
            list.style.display = "none";
          else
            list.style.display = "block";
    
          if (list.parentNode == body)
            break;
    
          listitem = list.parentNode;
          listitem.style.display = "list-item";
          list = listitem.parentNode;
        }
      }
      continue;
    }

    colon = listitem.id.indexOf(":");
    if (listitem.id.slice(colon + 1).search(name) == 0) {
      listitem.style.display = "list-item";
      hits.push(listitem);
    }
  }

  var protected = hits.slice(0);
  for (var iL = 0; iL != hits.length; ++iL) {
    listitem = hits[iL];
    list = listitem.parentNode;

    while (true) {
      list.style.display = "block";
      for (var iC = 0; iC != list.children.length; ++iC) {
        if (list.children[iC] != listitem && !isInList(list.children[iC], protected) && isVanishable(list.children[iC]))
          list.children[iC].style.display = "none";
      }

      protected.push(list.parentNode);

      if (list.parentNode == body)
        break;

      listitem = list.parentNode;
      listitem.style.display = "list-item";
      list = listitem.parentNode;
    }
  }
}
  </script>
 </head>
 <body>
  <div>
  Search dataset: <input type="text" size="100" onkeyup="searchDataset(this.value)" onpaste="searchDataset(this.value)" onchange="searchDataset(this.value)">
  </div>
  <ul>
'''

        html.write(text)

        keywords = {}

        for site_name in sorted(self.inventory_manager.sites.keys()):
            site = self.inventory_manager.sites[site_name]

            keywords['site'] = site.name
            keywords['site_cpu'] = cpu_before[site]
            keywords['storage_before'] = storage_before[site] * 100.
            keywords['storage_after'] = storage_after[site] * 100.

            text = '   <li><span onclick="toggleVisibility(\'{site}\')" class="menuitem">{site}</span>'.format(**keywords)
            text += ' (CPU: {site_cpu:.2f}, Storage: {storage_before:.2f}% -> {storage_after:.2f}%)\n'.format(**keywords)
            text += '    <ul id="{site}" class="collapsible">\n'.format(**keywords)

            site_replicas = []
            for dataset, usage_list in dataset_cpus.items():
                for ncpu, s in usage_list:
                    if s == site:
                        site_replicas.append((dataset, ncpu))
                        break

            site_replicas.sort(key = lambda (dataset, ncpu): ncpu, reverse = True)

            text += '     <li class="vanishable"><span onclick="toggleVisibility(\'{site}:existing\')" class="menuitem">Existing</span>\n'.format(**keywords)
            text += '      <ul id="{site}:existing" class="collapsible">\n'.format(**keywords)

            for dataset, ncpu in site_replicas:
                keywords['dataset'] = dataset.name
                keywords['occ'] = ncpu / site.cpu

                text += '       <li id="{site}:{dataset}" class="vanishable">'.format(**keywords)

                if ncpu / site.cpu > dealer_config.occupancy_fraction_threshold:
                    text += '<span class="popular">{dataset} ({occ:.2f})</span>'.format(**keywords)
                else:
                    text += '{dataset} ({occ:.2f})'.format(**keywords)

                text += '</li>\n'

            text += '      </ul>\n' # closing Existing list
            text += '     </li>\n' # item Existing

            site_copies = [r for r, origin in copy_list[site]]
            site_copies.sort(key = lambda r: r.size(), reverse = True)

            keywords['arrive_volume'] = sum(replica.size() * 1.e-9 for replica in site_copies)

            text += '     <li class="vanishable"><span onclick="toggleVisibility(\'{site}:arriving\')" class="menuitem">Arriving</span> {arrive_volume:.2f}\n'.format(**keywords)
            text += '      <ul id="{site}:arriving" class="arrive-bullet collapsible">\n'.format(**keywords)
            
            for replica in site_copies:
                keywords['dataset'] = replica.dataset.name
                keywords['size'] = replica.size() * 1.e-9
                text += '       <li id="{site}:{dataset}" class="vanishable">{dataset} ({size:.2f} GB)</li>\n'.format(**keywords)

            text += '      </ul>\n' # closing Arriving list
            text += '     </li>\n' # item Arriving

            text += '    </ul>\n' # site list (Existing / Arriving)
            text += '   </li>\n' # closes site
            
            html.write(text)

        text = '  </ul>\n'

        total_volume = 0.
        for replica_origins in copy_list.values():
            total_volume += sum(r.size() for r, o in replica_origins)

        text += '  <hr>\n'
        text += '  <p>Copy total: {total_volume} TB</p>\n'.format(total_volume = total_volume * 1.e-12)
        text += ''' </body>
</html>
'''

        html.write(text)

        html.close()

    def write_summary_by_requests(self, dataset_weights, copy_list, nfiles_before, storage_before, storage_after):
        html = open(dealer_config.summary_html, 'w')

        text = '''<html>
<html>
 <head>
  <title>Dealer copy decision records</title>
  <style>
body {
  font-family: Monospace;
  font-size: 120%;
}

span.menuitem {
  text-decoration: underline;
  cursor:pointer;
}

span.popular {
  color: red;
}

.collapsible {
  display: none;
}

li.vanishable {
  display: list-item;
}

ul.arrive-bullet {
  list-style-type: square;
}
  </style>
  <script type="text/javascript">
function toggleVisibility(id) {
  var list = document.getElementById(id);
  if (list.style.display == "block")
    list.style.display = "none";
  else
    list.style.display = "block";
}
function isCollapsible(obj) {
  var iC = 0;
  for (; iC != obj.classList.length; ++iC) {
    if (obj.classList[iC] == "collapsible")
      break;
  }
  return iC != obj.classList.length;
}
function isVanishable(obj) {
  var iC = 0;
  for (; iC != obj.classList.length; ++iC) {
    if (obj.classList[iC] == "vanishable")
      break;
  }
  return iC != obj.classList.length;
}
function isInList(obj, list) {
  var iC = 0;
  for (; iC != list.length; ++iC) {
    if (list[iC] == obj)
      break;
  }
  return iC != list.length;
}
function searchDataset(name) {
  if (name.length > 0 && name.length < 6)
    return;

  var listitems = document.getElementsByTagName("li");
  var listitem;
  var sublist;
  var colon;
  var list;
  var body = document.getElementsByTagName("body")[0];
  var hits = [];

  for (var iL = 0; iL != listitems.length; ++iL) {
    listitem = listitems[iL];

    if (name == "") {
      listitem.style.display = "list-item";

      list = listitem.parentNode;
      if (listitem == list.firstChild) {
        while (true) {
          if (isCollapsible(list))
            list.style.display = "none";
          else
            list.style.display = "block";
    
          if (list.parentNode == body)
            break;
    
          listitem = list.parentNode;
          listitem.style.display = "list-item";
          list = listitem.parentNode;
        }
      }
      continue;
    }

    colon = listitem.id.indexOf(":");
    if (listitem.id.slice(colon + 1).search(name) == 0) {
      listitem.style.display = "list-item";
      hits.push(listitem);
    }
  }

  var protected = hits.slice(0);
  for (var iL = 0; iL != hits.length; ++iL) {
    listitem = hits[iL];
    list = listitem.parentNode;

    while (true) {
      list.style.display = "block";
      for (var iC = 0; iC != list.children.length; ++iC) {
        if (list.children[iC] != listitem && !isInList(list.children[iC], protected) && isVanishable(list.children[iC]))
          list.children[iC].style.display = "none";
      }

      protected.push(list.parentNode);

      if (list.parentNode == body)
        break;

      listitem = list.parentNode;
      listitem.style.display = "list-item";
      list = listitem.parentNode;
    }
  }
}
  </script>
 </head>
 <body>
  <div>
  Search dataset: <input type="text" size="100" onkeyup="searchDataset(this.value)" onpaste="searchDataset(this.value)" onchange="searchDataset(this.value)">
  </div>
  <ul>
'''

        html.write(text)

        dataset_copies = collections.defaultdict(list)
        for site, replica_origins in copy_list.items():
            for replica, origin in replica_origins:
                dataset_copies[replica.dataset].append(replica)

        keywords = {}

        for site_name in sorted(self.inventory_manager.sites.keys()):
            site = self.inventory_manager.sites[site_name]

            keywords['site'] = site.name
            keywords['site_nfiles'] = nfiles_before[site] / site.cpu
            keywords['storage_before'] = storage_before[site] * 100.
            keywords['storage_after'] = storage_after[site] * 100.

            text = '   <li><span onclick="toggleVisibility(\'{site}\')" class="menuitem">{site}</span>'.format(**keywords)
            text += ' (Norm. Files: {site_nfiles:.2f}, Storage: {storage_before:.2f}% -> {storage_after:.2f}%)\n'.format(**keywords)
            text += '    <ul id="{site}" class="collapsible">\n'.format(**keywords)

            site_copies = [r for r, origin in copy_list[site]]
            site_copies.sort(key = lambda r: r.size(), reverse = True)

            site_replicas = []
            for dataset, weight in dataset_weights.items():
                for replica in dataset.replicas:
                    if replica.site == site and replica not in site_copies:
                        total_cpu = sum(r.site.cpu for r in dataset.replicas)
                        site_replicas.append((dataset, weight, total_cpu))
                        break

            site_replicas.sort(key = lambda (dataset, weight, total_cpu): weight, reverse = True)

            text += '     <li class="vanishable"><span onclick="toggleVisibility(\'{site}:existing\')" class="menuitem">Existing</span>\n'.format(**keywords)
            text += '      <ul id="{site}:existing" class="collapsible">\n'.format(**keywords)

            for dataset, weight, total_cpu in site_replicas:
                keywords['dataset'] = dataset.name
                keywords['weight'] = weight
                keywords['num_files'] = dataset.num_files * site.cpu / total_cpu

                text += '       <li id="{site}:{dataset}" class="vanishable">'.format(**keywords)

                if weight > len(dataset.replicas) - len(dataset_copies[dataset]):
                    text += '<span class="popular">{dataset} ({weight:.2f}, {num_files:.2f})</span>'.format(**keywords)
                else:
                    text += '{dataset} ({weight:.2f}, {num_files:.2f})'.format(**keywords)

                text += '</li>\n'

            text += '      </ul>\n' # closing Existing list
            text += '     </li>\n' # item Existing

            keywords['arrive_volume'] = sum(replica.size() * 1.e-9 for replica in site_copies)

            text += '     <li class="vanishable"><span onclick="toggleVisibility(\'{site}:arriving\')" class="menuitem">Arriving</span> {arrive_volume:.2f}\n'.format(**keywords)
            text += '      <ul id="{site}:arriving" class="arrive-bullet collapsible">\n'.format(**keywords)
            
            for replica in site_copies:
                keywords['dataset'] = replica.dataset.name
                keywords['size'] = replica.size() * 1.e-9
                text += '       <li id="{site}:{dataset}" class="vanishable">{dataset} ({size:.2f} GB)</li>\n'.format(**keywords)

            text += '      </ul>\n' # closing Arriving list
            text += '     </li>\n' # item Arriving

            text += '    </ul>\n' # site list (Existing / Arriving)
            text += '   </li>\n' # closes site
            
            html.write(text)

        text = '  </ul>\n'

        total_volume = 0.
        for replica_origins in copy_list.values():
            total_volume += sum(r.size() for r, o in replica_origins)

        text += '  <hr>\n'
        text += '  <p>Copy total: {total_volume} TB</p>\n'.format(total_volume = total_volume * 1.e-12)
        text += ''' </body>
</html>
'''

        html.write(text)

        html.close()
