import time
import logging
import datetime

from common.dataformat import DatasetReplica
import common.configuration as config
import dealer.configuration as dealer_config

logger = logging.getLogger(__name__)

class Dealer(object):

    def __init__(self, inventory, transaction, demand):
        self.inventory_manager = inventory
        self.transaction_manager = transaction
        self.demand_manager = demand

        self.copy_message = 'DynaMO -- Automatic Replication Request.'

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
#        self.demand_manager.update(self.inventory_manager)

        copy_list = self.determine_copies()

        self.commit_copies(copy_list)

    def determine_copies(self):
        """
        Simplest algorithm:
        1. Compute time- and slot- normalized CPU hour (site occupation rate) for each dataset replica.
        2. Sum up occupancy for each site.
        3. At each site, datasets with occupation rate > threshold are placed in the list of copy candidates.
        #5. Sum up occupation rates for each dataset.
        #6. Datasets whose global occupation rates are increasing are placed in the list of copy candidates.
        """

        copy_list = []

        now = time.time() # UNIX timestamp of now
        utc = time.gmtime(now) # struct_time in UTC
        utctoday = datetime.date(utc.tm_year, utc.tm_mon, utc.tm_mday)
        last_three_days = [utctoday - datetime.timedelta(2), utctoday - datetime.timedelta(1), utctoday]

        group = self.inventory_manager.groups[dealer_config.operating_group]

        replica_cpus = []
        site_cpu_occupancies = {}
        site_storage = {}

        for site in self.inventory_manager.sites.values():
            logger.info('Inspecting replicas at %s', site.name)

            site_occupancy = 0.
                
            for replica in site.dataset_replicas:
                accesses = replica.accesses[DatasetReplica.ACC_LOCAL]

                if len(accesses) == 0:
                    continue

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

                if dataset_ncpu == 0.:
                    continue

                logger.info('%s has time-normalized CPU usage of %f', replica.dataset.name, dataset_ncpu)

                replica_cpus.append((replica, dataset_ncpu))

            site_cpu_occupancies[site] = site_occupancy

            site_storage[site] = site.storage_occupancy(group)

            logger.info('%s occupancy: %f (CPU) %f (storage)', site.name, site_cpu_occupancies[site], site_storage[site])

        self.write_html_summary(replica_cpus, site_cpu_occupancies, site_storage)

        for replica, dataset_ncpu in replica_cpus:
            dataset = replica.dataset
            site = replica.site

            if dataset_ncpu / site.cpu < dealer_config.max_occupation_rate:
                continue

            logger.info('%s is busy.', dataset.name)

            if len(dataset.replicas) > dealer_config.max_replicas:
                logger.warning('%s has too many replicas already. Not copying.', dataset.name)
                continue

            sorted_sites = sorted(site_cpu_occupancies.items(), key = lambda (s, o): o) #sorted from emptiest to busiest

            destination_site = None
            for dest, occ in sorted_sites:
                if site_storage[dest] < config.target_site_occupancy and dest.find_dataset_replica(dataset) is None:
                    destination_site = dest
                    break

            if destination_site is None:
                logger.warning('No site to copy %s was found.', dataset.name)
                continue

            logger.info('Copying %s from %s to %s', dataset.name, site.name, destination_site.name)

            new_replica = self.inventory_manager.add_dataset_to_site(dataset, destination_site, group)
            copy_list.append((new_replica, site))

            # add the size of this replica to destination storage
            site_storage[destination_site] += new_replica.size()
            
            # add the cpu usage of this replica to destination occupancy
            site_cpu_occupancies[destination_site] += dataset_ncpu / destination_site.cpu

        return copy_list

    def commit_copies(self, copy_list):
        self.transaction_manager.copy.schedule_copies(copy_list, comments = self.copy_message)

        self.inventory_manager.store.add_dataset_replicas([replica for replica, origin in copy_list])

    def write_html_summary(self, replica_cpus, site_cpu_occupancies, site_storage):
        html = open('/var/www/html/dealer/copy_decisions.html', 'w')

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

span.busy {
  color: red;
}

ul.collapsible {
  display: none;
}

li.vanishable {
  display: list-item;
}
  </style>
  <script type="text/javascript">
function toggleVisibility(id) {
  list = document.getElementById(id);
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
    sublist = null;
    for (var iC = 0; iC != listitem.children.length; ++iC) {
      if (listitem.children[iC].tagName == "UL")
        sublist = listitem.children[iC];
    }

    if (name == "") {
      listitem.style.display = "list-item";
      if (sublist !== null && isCollapsible(sublist))
        sublist.style.display = "none";

      continue;
    }

    if (sublist === null)
      continue;

    colon = sublist.id.indexOf(":");
    if (sublist.id.slice(colon + 1).search(name) == 0) {
      listitem.style.display = "list-item";
      sublist.style.display = "block";
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

        for site_name in sorted(self.inventory_manager.sites.keys()):
            site = self.inventory_manager.sites[site_name]

            site_replicas = [(r, ncpu) for r, ncpu in replica_cpus if r.site == site]
            site_replicas.sort(key = lambda (r, ncpu): ncpu, reverse = True)

            text = '   <li class="vanishable"><span onclick="toggleVisibility(\'{site}\')" class="menuitem">{site}</span>'.format(site = site.name)
            text += ' (CPU: {cpu:.2f}, Storage: {storage:.2f}%)\n'.format(cpu = site_cpu_occupancies[site], storage = site_storage[site] * 100.)
            text += '    <ul id="{site}" class="collapsible">\n'.format(site = site.name)

            for replica, ncpu in site_replicas:
                dataset = replica.dataset

                text += '     <li class="vanishable">'
                entry = '%s (%f)' % (dataset.name, ncpu / site.cpu)

                if ncpu / site.cpu > dealer_config.max_occupation_rate:
                    text += '<span class="busy">%s</span>' % entry
                else:
                    text += entry

                text += '</li>\n'

            text += '    </ul>\n'
            text += '   </li>\n'
            
            html.write(text)

        text = '''  </ul>
 </body>
</html>
'''

        html.write(text)

        html.close()
