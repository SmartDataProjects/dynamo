import time
import logging
import math
import pprint
import collections

import common.configuration as config
import detox.policy as policy
import detox.configuration as detox_config

logger = logging.getLogger(__name__)

class Detox(object):

    def __init__(self, inventory, transaction, demand, history, log_path = detox_config.log_path, html_path = detox_config.html_path):
        self.inventory_manager = inventory
        self.transaction_manager = transaction
        self.demand_manager = demand
        self.history = history

        self.policy_managers = {} # {partition: PolicyManager}
        self.quotas = {} # {partition: {site: quota}}
        self.policy_log_path = log_path
        self.policy_html_path = html_path

        self.deletion_message = 'Dynamo -- Automatic Cache Release Request.'

    def set_policies(self, policy_stack, quotas, partition = ''): # empty partition name -> default
        self.policy_managers[partition] = policy.PolicyManager(policy_stack)
        self.quotas[partition] = quotas

    def run(self, partition = '', dynamic_deletion = True):
        """
        Main executable.
        """

        if partition:
            logger.info('Detox run for %s starting at %s', partition, time.strftime('%Y-%m-%d %H:%M:%S'))
        else:
            logger.info('Detox run starting at %s', time.strftime('%Y-%m-%d %H:%M:%S'))

        if time.time() - self.inventory_manager.store.last_update > config.inventory.refresh_min:
            logger.info('Inventory was last updated at %s. Reloading content from remote sources.', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.inventory_manager.store.last_update)))
            # inventory is stale -> update
            self.inventory_manager.update()

        self.demand_manager.update(self.inventory_manager)
        self.inventory_manager.site_source.set_site_status(self.inventory_manager.sites) # update site status regardless of inventory updates

        policy_log = None
        if self.policy_log_path:
            policy_log = open(self.policy_log_path, 'w')

        all_deletions = []
        iteration = 0

        logger.info('Start deletion. Evaluating %d policies against %d replicas.', self.policy_managers[partiton].num_policies(), sum([len(d.replicas) for d in self.inventory_manager.datasets.values()]))

        protection_list = []

        while True:
            iteration += 1

            records = []
            candidates = self.find_candidates(partition, protection_list, records)

            if policy_log:
                logger.info('Writing policy hit log text.')
                self.write_policy_log(policy_log, iteration, records)

            logger.info('%d dataset replicas in deletion list', len(candidates))
            if logger.getEffectiveLevel() == logging.DEBUG:
                logger.debug('Deletion list:')
                logger.debug(pprint.pformat(['%s:%s' % (r.site.name, r.dataset.name) for r in candidates]))

            if dynamic_deletion:
                replica = self.select_replica(partition, candidates, protection_list)
                if replica is None:
                    deletion_list = []
                else:
                    logger.info('Selected replica: %s %s', replica.site.name, replica.dataset.name)
                    deletion_list = [replica]

            else:
                deletion_list = candidates

            if len(deletion_list) == 0:
                break

            all_deletions.extend(deletion_list)

            for replica in deletion_list:
                self.inventory_manager.unlink_datasetreplica(replica)

        # fetch the copy/deletion run number
        run_number = self.history.new_deletion_run(partition)

        self.history.save_deletion_decisions(run_number, all_deletions, protection_list, self.inventory_manager)

        logger.info('Committing deletion.')
        self.commit_deletions(run_number, all_deletions)

        if policy_log:
            policy_log.close()

        logger.info('Detox run finished at %s', time.strftime('%Y-%m-%d %H:%M:%S'))

    def find_candidates(self, partition, protection_list, records = None):
        """
        Run each dataset / block replicas through deletion policies and make a list of replicas to delete.
        Return the list of replicas that may be deleted (deletion_candidates) or must be protected (protection_list).
        """

        candidates = []

        for dataset in self.inventory_manager.datasets.values():
            for replica in dataset.replicas:
                if replica in protection_list:
                    continue

                hit_records = self.policy_managers[partition].decision(replica, self.demand_manager)

                if records is not None:
                    records.append(hit_records)

                decision = hit_records.decision()

                if decision == policy.DEC_DELETE:
                    candidates.append(replica)

                elif decision == policy.DEC_PROTECT:
                    protection_list.append(replica)
                
        return candidates

    def commit_deletions(self, run_number, all_deletions):
        # first make sure the list of blocks is up-to-date
        datasets = list(set(r.dataset for r in all_deletions))
        self.inventory_manager.dataset_source.set_dataset_constituent_info(datasets)

        sites = set(r.site for r in all_deletions)

        # now schedule deletion for each site
        for site in sorted(sites):
            replica_list = [r for r in all_deletions if r.site == site]

            logger.info('Deleting %d replicas from %s.', len(replica_list), site.name)

            deletion_mapping = self.transaction_manager.deletion.schedule_deletions(replica_list, comments = self.deletion_message)
            # deletion_mapping .. {deletion_id: (approved, [replicas])}

            for deletion_id, (approved, replicas) in deletion_mapping.items():
                if approved:
                    self.inventory_manager.store.delete_datasetreplicas(replicas)
                    self.inventory_manager.store.set_last_update()

                size = sum([r.size() for r in replicas])

                self.history.make_deletion_entry(run_number, site, deletion_id, approved, [r.dataset for r in replicas], size)

            logger.info('Done deleting %d replicas from %s.', len(replica_list), site.name)

    def select_replica(self, partition, deletion_candidates, protection_list):
        """
        Select one dataset replica to delete out of all deletion candidates.
        Currently returning the smallest replica on the site with the highest protected fraction.
        Ranking policy here may be made dynamic at some point.
        """

        if len(deletion_candidates) == 0:
            return None

        protection_by_site = collections.defaultdict(list)
        for replica in protection_list:
            protection_by_site[replica.site].append(replica)

        # find the site with the highest protected fraction
        target_site = max(protection_by_site.keys(), key = lambda site: sum(replica.size() for replica in protection_by_site[site]) / self.quotas[partition][site])

        # return the smallest replica on the target site
        return min(protection_by_site[target_site], key = lambda replica: replica.size())

    def write_policy_log(self, policy_log, iteration, all_records):
        policy_log.write('===== Begin iteration %d =====\n\n' % iteration)

        for site in sorted(self.inventory_manager.sites.values(), key = lambda s: s.name):
            site_records = filter(lambda record: record.replica.site == site, all_records)

            for record in sorted(site_records, key = lambda rec: rec.replica.dataset.name):
                record.write_records(policy_log)

        policy_log.write('\n===== End iteration %d =====\n\n' % iteration)

    def open_html(self, path):
        html = open(path, 'w')

        text = '''<html>
<html>
 <head>
  <title>Detox policy hit records</title>
  <style>
body {
  font-family: Monospace;
  font-size: 120%;
}

span.menuitem {
  text-decoration: underline;
  cursor:pointer;
}

span.PROTECT {
  color: purple;
}

span.KEEP {
  color: blue;
}

span.DELETE {
  color: red;
}

span.NEUTRAL {
  color: black;
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
'''  

        html.write(text)
        return html

    def close_html(self, html):
        if not html:
            return

        text = '''
 </body>
</html>
'''

        html.write(text)
        html.close()

    def write_html_iteration(self, html, iteration, all_records):
        """
        Write an html block for the specific iteration. Loop over sites, decisions, datasets, policies.
        <ul>
         <li>site
          <ul>
           <li>decision
            <ul>
             <li>dataset
              <ul>
               <li>policy</li>
              </ul>
             </li>
            </ul>
           </li>
          </ul>
         </li>
        """

        GB = 1000. * 1000. * 1000.
        TB = GB * 1000.

        html_lines = []
        keywords = {}
        global_space = dict((dec, 0) for dec in policy.DECISIONS)
        policy_protection_size = {} # policy -> total size of protected data

        # loop over sites (sorted by name)
        for site in sorted(self.inventory_manager.sites.values(), key = lambda s: s.name):
            decision_htmls = []
            used_space = dict((dec, 0) for dec in policy.DECISIONS)

            keywords['site'] = site.name

            # loop over decisions
            for decision in policy.DECISIONS:
                site_records = filter(lambda hit_records: hit_records.replica.site == site and hit_records.decision() == decision, all_records)
                site_records.sort(key = lambda hit_records: hit_records.replica.dataset.name)

                replica_htmls = []

                decision_str = policy.DECISION_STR[decision]
                keywords['decision'] = decision_str
                if decision == policy.DEC_NEUTRAL:
                    keywords['menuitem'] = ''
                else:
                    keywords['menuitem'] = ' menuitem'

                # loop over replicas
                for hit_records in site_records:
                    replica = hit_records.replica
                    dataset = replica.dataset
                    replica_size = replica.size()

                    keywords['dataset'] = dataset.name
                    keywords['replica_size'] = replica_size / GB

                    used_space[decision] += replica_size

                    replica_html = []
                    replica_html.append('<li class="vanishable"><span onclick="toggleVisibility(\'{site}:{dataset}\')" class="{decision}{menuitem}">{dataset}</span> ({replica_size:.1f} GB)')
                    replica_html.append(' <ul id="{site}:{dataset}" class="collapsible">\n')
                    replica_html += ['  <li>{policy}:{decision} ({reason})</li>'.format(policy = record.policy.name, decision = policy.DECISION_STR[record.decision], reason = record.reason) for record in hit_records.records]
                    replica_html.append(' </ul>')
                    replica_html.append('</li>')

                    replica_htmls += map(lambda l: l.format(**keywords), replica_html)

                    # add the replica size to protection tally
                    for record in hit_records.records:
                        if record.decision == policy.DEC_PROTECT:
                            try:
                                policy_protection_size[record.policy] += replica_size
                            except KeyError:
                                policy_protection_size[record.policy] = replica_size

                keywords['used_space'] = used_space[decision] / TB

                # make html for the decision block
                decision_html = []
                decision_html.append('<li class="vanishable"><span onclick="toggleVisibility(\'{site}:{decision}\')" class="menuitem">{decision}</span> ({used_space:.1f} TB)')
                decision_html.append(' <ul id="{site}:{decision}" class="collapsible">')
                for line in replica_htmls:
                    decision_html.append('  ' + line)
                decision_html.append(' </ul>')
                decision_html.append('</li>')

                decision_htmls += map(lambda l: l.format(**keywords), decision_html)

            # site storage
            for dec in policy.DECISIONS:
                decision_str = policy.DECISION_STR[dec]
                keywords['used_space_' + decision_str] = used_space[dec] / TB

                global_space[dec] += used_space[dec]

            # make html for the site
            site_html = []
            line = '<li class="vanishable"><span onclick="toggleVisibility(\'{site}\')" class="menuitem">{site}</span>'
            line += ' (<span class="NEUTRAL">N:{used_space_NEUTRAL:.1f}</span>, <span class="DELETE">D:{used_space_DELETE:.1f}</span>, <span class="KEEP">K:{used_space_KEEP:.1f}</span>, <span class="PROTECT">P:{used_space_PROTECT:.1f}</span> TB)\n'
            site_html.append(line)
            site_html.append(' <ul id="{site}" class="collapsible">')
            for line in decision_htmls:
                site_html.append('  ' + line)
            site_html.append(' </ul>')
            site_html.append('</li>')

            html_lines += map(lambda l: l.format(**keywords), site_html)

        html.write('  <h2>Iteration %d</h2>\n' % iteration)
        html.write('  <ul id="maintable">\n')
        for line in html_lines:
            html.write('    ' + line + '\n')
        html.write('  </ul>\n')
        html.write('  <h3>Global space usage</h3>\n')
        html.write('  <ul id="global_space_usage">\n')
        for dec in policy.DECISIONS:
            html.write('   <li>%s: %.1f TB</li>\n' % (policy.DECISION_STR[dec], global_space[dec] / TB))
        html.write('  </ul>\n')
        html.write('  <h3>Protected sizes by each policy</h3>\n')
        html.write('  <ul id="protection_summary">\n')
        for pol, size in sorted(policy_protection_size.items(), key = lambda (pol, size): size, reverse = True):
            html.write('   <li>%s: %.1f TB</li>\n' % (pol.name, size / TB))
        html.write('  </ul>\n')
        html.write('  <hr>\n')


if __name__ == '__main__':

    import sys
    from argparse import ArgumentParser

    from common.inventory import InventoryManager
    from common.transaction import TransactionManager
    from common.demand import DemandManager
    import common.interface.classes as classes
    from detox.policies import ActionList

    parser = ArgumentParser(description = 'Use detox')

    parser.add_argument('replica', metavar = 'SITE:DATASET', help = 'Replica to delete.')
    parser.add_argument('--commit', '-C', action = 'store_true', dest = 'commit',  help = 'Actually make deletion.')
    parser.add_argument('--log-level', '-l', metavar = 'LEVEL', dest = 'log_level', default = '', help = 'Logging level.')

    args = parser.parse_args()
    sys.argv = []

    site_pattern, sep, dataset_pattern = args.replica.partition(':')
    
    if args.log_level:
        try:
            level = getattr(logging, args.log_level.upper())
            logging.getLogger().setLevel(level)
        except AttributeError:
            logging.warning('Log level ' + args.log_level + ' not defined')
    
    kwd = {}
    for cls in ['store', 'site_source', 'dataset_source', 'replica_source']:
        kwd[cls + '_cls'] = classes.default_interface[cls]
    
    inventory_manager = InventoryManager(**kwd)
    
    transaction_manager = TransactionManager()
    
    demand_manager = DemandManager()

    history = classes.default_interface['history']()

    detox = Detox(inventory_manager, transaction_manager, demand_manager, history)

    action_list = ActionList()
    action_list.add_action('Delete', site_pattern, dataset_pattern)

    detox.set_policies(action_list)

    if not args.commit:
        config.read_only = True

    detox.run(dynamic_deletion = False)
