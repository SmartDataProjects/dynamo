import time
import logging
import math
import pprint

import common.configuration as config
import detox.policy as policy
import detox.configuration as detox_config

logger = logging.getLogger(__name__)

class Detox(object):

    def __init__(self, inventory, transaction, demand, policies, log_path = detox_config.log_path, html_path = detox_config.html_path):
        self.inventory_manager = inventory
        self.transaction_manager = transaction
        self.demand_manager = demand
        self.policy_manager = policy.PolicyManager(policies)
        self.policy_log_path = log_path
        self.policy_html_path = html_path

        self.deletion_message = 'DynaMO -- Automatic Cache Release Request. Summary at: http://t3serv001.mit.edu/~cmsprod/IntelROCCS/Detox/result/'

    def run(self, dynamic_deletion = True):
        """
        Main executable.
        """

        logger.info('Detox run starting at %s', time.strftime('%Y-%m-%d %H:%M:%S'))

        if time.time() - self.inventory_manager.inventory.last_update > config.inventory.refresh_min:
            logger.info('Inventory was last updated at %s. Reloading content from remote sources.', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.inventory_manager.inventory.last_update)))
            # inventory is stale -> update
            self.inventory_manager.update()

        else:
            self.inventory_manager.load()

        self.demand_manager.update(self.inventory_manager.inventory)

        logger.info('Start deletion.')

        policy_log = None
        if self.policy_log_path:
            policy_log = open(self.policy_log_path, 'w')

        html = None
        if self.policy_html_path:
            html = self.open_html(self.policy_html_path)

        all_deletions = {} # site -> list of replicas to delete on site
        iteration = 0

        while True:
            iteration += 1

            records = []
            deletion_list, protection_list = self.make_deletion_protection_list(records)

            if policy_log:
                self.write_policy_log(policy_log, iteration, records)

            if html:
                self.write_html_iteration(html, iteration, records)

            logger.info('%d dataset replicas in deletion list', len(deletion_list))
            if logger.getEffectiveLevel() == logging.DEBUG:
                logger.debug('Deletion list:')
                logger.debug(pprint.pformat(['%s:%s' % (r.site.name, r.dataset.name) for r in deletion_list]))

            if len(deletion_list) == 0:
                break

            if dynamic_deletion:
                self.dynamic_deletion(deletion_list, protection_list, all_deletions)

            else:
                self.static_deletion(deletion_list, all_deletions)
                break

        logger.info('Committing deletion.')
        self.commit_deletions(all_deletions)

        if policy_log:
            policy_log.close()

        self.close_html(html)

        logger.info('Detox run finished at %s', time.strftime('%Y-%m-%d %H:%M:%S'))

    def make_deletion_protection_list(self, records = None):
        """
        Run each dataset / block replicas through deletion policies and make a list of replicas to delete.
        Return the list of replicas that may be deleted and must be protected.
        """

        deletion_list = []
        protection_list = []

        for dataset in self.inventory_manager.datasets.values():
            for replica in dataset.replicas:
                hit_records = self.policy_manager.decision(replica, self.demand_manager)

                if records is not None:
                    records.append(hit_records)

                decision = hit_records.decision()

                if decision == policy.DEC_DELETE:
                    deletion_list.append(replica)

                elif decision == policy.DEC_PROTECT:
                    protection_list.append(replica)
                
        return deletion_list, protection_list

    def dynamic_deletion(self, deletion_list, protection_list, all_deletions):
        replica = self.select_replica(deletion_list, protection_list)
        logger.info('Selected replica: %s %s', replica.site.name, replica.dataset.name)

        try:
            all_deletions[replica.site].append(replica)
        except KeyError:
            all_deletions[replica.site] = [replica]

        self.inventory_manager.delete_replica(replica)

    def static_deletion(self, deletion_list, all_deletions):
        for replica in deletion_list:
            try:
                all_deletions[replica.site].append(replica)
            except KeyError:
                all_deletions[replica.site] = [replica]

            # TESTING
            if replica.site.name != 'T2_US_MIT':
                continue
            # TESTING

            self.inventory_manager.delete_replica(replica)

    def commit_deletions(self, all_deletions):
        for site, replica_list in all_deletions.items():
            # TESTING
            if site.name != 'T2_US_MIT':
                continue
            # TESTING

            self.transaction_manager.delete_many(replica_list, comments = self.deletion_message)

            logger.info('Deleted %d replicas from %s.', len(replica_list), site.name)

    def select_replica(self, deletion_list, protection_list):
        """
        Select one dataset replica to delete out of all deletion candidates.
        Currently returning the replica whose deletion balances the protected fraction between the sites the most.
        Ranking policy here may be made dynamic at some point.
        """

        if len(deletion_list) == 0:
            return None
        
        protected_fractions = {}
        for replica in protection_list:
            try:
                protected_fractions[replica.site] += replica.size() / replica.site.capacity
            except KeyError:
                protected_fractions[replica.site] = replica.size() / replica.site.capacity

        for site in self.inventory_manager.sites.values():
            if site not in protected_fractions: # highly unlikely
                protected_fractions[site] = 0.

        # Select the replica that minimizes the RMS of the protected fractions
        minRMS2 = -1.
        deletion_candidate = None

        for replica in deletion_list:
            sumf2 = sum([frac * frac for site, frac in protected_fractions.items() if site != replica.site])
            sumf = sum([frac for site, frac in protected_fractions.items() if site != replica.site])

            sumf2 += math.pow(protected_fractions[replica.site] - replica.size() / replica.site.capacity, 2.)
            sumf += protected_fractions[replica.site] - replica.size() / replica.site.capacity

            rms2 = sumf2 / len(self.inventory_manager.sites) - math.pow(sumf / len(self.inventory_manager.sites), 2.)

            if minRMS2 < 0. or rms2 < minRMS2:
                minRMS2 = rms2
                deletion_candidate = replica

        return deletion_candidate

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
  text-decoration: underline;
  cursor:pointer;
}

span.KEEP {
  color: blue;
  text-decoration: underline;
  cursor:pointer;
}

span.DELETE {
  color: red;
  text-decoration: underline;
  cursor:pointer;
}

span.NEUTRAL {
  color: black;
}

ul.collapsible {
  display: none;
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
function searchDataset(name) {
  if (name.length > 0 && name.length < 6)
    return;

  var lists = document.getElementsByTagName("ul");
  var list;
  var col;
  var parent;
  var body = document.getElementsByTagName("body")[0];
  for (var iL in lists) {
    list = lists[iL];
    if (name == "") {
      if (list.id == "maintable" || list.id == "global_space_usage" || list.id == "protection_summary")
        list.style.display = "block";
      else
        list.style.display = "none";

      continue;
    }
    col = list.id.indexOf(":");
    if (col != -1) {
      if (list.id.slice(col + 1).search(name) == 0) {
        parent = list;
        while (parent != body) {
          if (parent.tagName == "UL")
            parent.style.display = "block";
  
          parent = parent.parentNode;
        }
        
        continue;
      }
    }

    list.style.display = "none";
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

                # loop over replicas
                for hit_records in site_records:
                    replica = hit_records.replica
                    dataset = replica.dataset
                    replica_size = replica.size()

                    keywords['dataset'] = dataset.name
                    keywords['replica_size'] = replica_size / GB

                    used_space[decision] += replica_size

                    replica_html = []
                    replica_html.append('<li><span onclick="toggleVisibility(\'{site}:{dataset}\')" class="{decision}">{dataset}</span> ({replica_size:.1f} GB)')
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
                decision_html.append('<li><span onclick="toggleVisibility(\'{site}:{decision}\')" class="menuitem">{decision}</span> ({used_space:.1f} TB)')
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
            line = '<li><span onclick="toggleVisibility(\'{site}\')" class="menuitem">{site}</span>'
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
    for cls in ['inventory', 'site_source', 'dataset_source', 'replica_source']:
        kwd[cls + '_cls'] = classes.default_interface[cls]
    
    inventory_manager = InventoryManager(**kwd)
    
    transaction_manager = TransactionManager()
    
    demand_manager = DemandManager()

    action_list = ActionList()
    action_list.add_action('Delete', site_pattern, dataset_pattern)

    detox = Detox(inventory_manager, transaction_manager, demand_manager, [action_list])
    if not args.commit:
        config.read_only = True

    detox.run(dynamic_deletion = False)
