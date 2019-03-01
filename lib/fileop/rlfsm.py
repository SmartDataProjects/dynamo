import os
import collections
import random
import time
import datetime
import threading
import logging
import datetime
import time

from dynamo.fileop.base import FileQuery
from dynamo.fileop.transfer import FileTransferOperation, FileTransferQuery
from dynamo.fileop.deletion import FileDeletionOperation, FileDeletionQuery, DirDeletionOperation
from dynamo.fileop.errors import irrecoverable_errors
from dynamo.dataformat import Configuration, Block, Site, BlockReplica
from dynamo.history.history import HistoryDatabase
from dynamo.utils.interface.mysql import MySQL
from dynamo.policy.condition import Condition
from dynamo.policy.variables import site_variables

LOG = logging.getLogger(__name__)

class RLFSM(object):
    """
    File operations manager using MySQL tables for queue bookkeeping. Also implies the
    inventory backend is MySQL.
    """

    class Subscription(object):
        __slots__ = ['id', 'status', 'file', 'destination', 'disk_sources', 'tape_sources', 'failed_sources', 'hold_reason']

        def __init__(self, id, status, file, destination, disk_sources, tape_sources, failed_sources = None, hold_reason = None):
            self.id = id
            self.status = status
            self.file = file
            self.destination = destination
            self.disk_sources = disk_sources
            self.tape_sources = tape_sources
            self.failed_sources = failed_sources
            self.hold_reason = hold_reason

    class TransferTask(object):
        __slots__ = ['id', 'subscription', 'source']

        def __init__(self, subscription, source):
            self.id = None
            self.subscription = subscription
            self.source = source

        def __str__(self):
            s = ''
            s += self.source.name
            s += '->'
            s += self.subscription.destination.name
            s += ' ' + self.subscription.file.lfn
            return s

    class Desubscription(object):
        __slots__ = ['id', 'status', 'file', 'site']

        def __init__(self, id, status, file, site):
            self.id = id
            self.status = status
            self.file = file
            self.site = site

    class DeletionTask(object):
        __slots__ = ['id', 'desubscription']

        def __init__(self, desubscription):
            self.id = None
            self.desubscription = desubscription

    # default config
    _config = ''

    @staticmethod
    def set_default(config):
        RLFSM._config = Configuration(config)

    def __init__(self, config = None):
        if config is None:
            config = RLFSM._config

        # Handle to the inventory DB
        self.db = MySQL(config.db.db_params)

        # Handle to the history DB
        self.history_db = HistoryDatabase(config.get('history', None))

        # FileTransferOperation backend (can make it a map from (source, dest) to operator)
        self.transfer_operations = []
        if 'transfer' in config:
            for condition_text, module, conf in config.transfer:
                if condition_text is None: # default
                    condition = None
                else:
                    condition = Condition(condition_text, site_variables)

                self.transfer_operations.append((condition, FileTransferOperation.get_instance(module, conf)))
            
        if 'transfer_query' in config:
            self.transfer_queries = []
            for condition_text, module, conf in config.transfer_query:
                if condition_text is None: # default
                    condition = None
                else:
                    condition = Condition(condition_text, site_variables)

                self.transfer_queries.append(condition, FileTransferQuery.get_instance(module, conf))
        else:
            self.transfer_queries = self.transfer_operations

        if 'deletion' in config:
            self.deletion_operations = []
            for condition_text, module, conf in config.deletion:
                if condition_text is None: # default
                    condition = None
                else:
                    condition = Condition(condition_text, site_variables)

                self.deletion_operations.append(condition, FileDeletionOperation.get_instance(module, conf))
        else:
            self.deletion_operations = self.transfer_operations

        if 'deletion_query' in config:
            self.deletion_queries = []
            for condition_text, module, conf in config.deletion_query:
                if condition_text is None: # default
                    condition = None
                else:
                    condition = Condition(condition_text, site_variables)

                self.deletion_queries.append(condition, FileDeletionQuery.get_instance(module, conf))
        else:
            self.deletion_queries = self.deletion_operations

        self.sites_in_downtime = []

        # Cycle thread
        self.main_cycle = None
        self.cycle_stop = threading.Event()

        self.set_read_only(config.get('read_only', False))

    def set_read_only(self, value = True):
        self._read_only = value
        self.history_db.set_read_only(value)
        for _, op in self.transfer_operations:
            op.set_read_only(value)
        if self.transfer_queries is not self.transfer_operations:
            for _, qry in self.transfer_queries:
                qry.set_read_only(value)
        if self.deletion_operations is not self.transfer_operations:
            for _, op in self.deletion_operations:
                op.set_read_only(value)
        if self.deletion_queries is not self.deletion_operations:
            for _, qry in self.deletion_queries:
                qry.set_read_only(value)

    def start(self, inventory):
        """
        Start the file operations management cycle. Issue transfer and deletion tasks to the backend.
        """

        if self.main_cycle is not None:
            return

        LOG.info('Starting file operations manager')

        self.main_cycle = threading.Thread(target = self._run_cycle, name = 'FOM', args = (inventory,))
        self.main_cycle.start()

        LOG.info('Started file operations manager.')

    def stop(self):
        """
        Stop the file operations management cycle.
        """

        LOG.info('Stopping file operations manager.')

        self.cycle_stop.set()
        self.main_cycle.join()

        self.main_cycle = None
        self.cycle_stop.clear()
        
    def transfer_files(self, inventory):
        """
        Routine for managing file transfers.
        1. Query the file transfer agent and update the status of known subscriptions.
        2. Pick up new subscriptions.
        3. Select the source for each transfer.
        4. Organize the transfers into batches.
        5. Start the transfers.
        The routine can be stopped almost at any point without corrupting the state machine.
        The only cases where stopping is problematic are within self._update_status and self._start_transfers.

        @param inventory   The inventory.
        """
        
        self._cleanup()

        LOG.debug('Clearing cancelled transfer tasks.')
        task_ids = self._get_cancelled_tasks('transfer')
        for _, op in self.transfer_operations:
            op.cancel_transfers(task_ids)

        if self.cycle_stop.is_set():
            return

        LOG.debug('Fetching subscription status from the file operation agent.')
        self._update_status('transfer', inventory)

        if self.cycle_stop.is_set():
            return

        LOG.debug('Filtering out transfers to unavailable destinations.')
        if not self._read_only:
            for site in self.sites_in_downtime:
                self.db.query('UPDATE `file_subscriptions` SET `status` = \'held\', `hold_reason` = \'site_unavailable\' WHERE `site_id` = (SELECT `id` FROM `sites` WHERE `name` = %s)', site.name)

        if self.cycle_stop.is_set():
            return

        LOG.debug('Collecting new transfer subscriptions.')
        subscriptions = self.get_subscriptions(inventory, op = 'transfer', status = ['new', 'retry'])

        if self.cycle_stop.is_set():
            return

        # We check the operators here because get_subscriptions does some state update and we want that to happen
        pending_count = {}
        n_available = 0
        for _, op in self.transfer_operations:
            pending_count[op] = op.num_pending_transfers()
            if pending_count[op] <= op.max_pending_transfers:
                n_available += 1

        if n_available == 0:
            LOG.info('No transfer operators are available at the moment.')
            return

        LOG.debug('Identifying source sites for %d transfers.', len(subscriptions))
        tasks = self._select_source(subscriptions)

        if self.cycle_stop.is_set():
            return

        LOG.debug('Organizing %d transfers into batches.', len(tasks))

        by_dest = {}
        for task in tasks:
            try:
                by_dest[task.subscription.destination].append(task)
            except KeyError:
                by_dest[task.subscription.destination] = [task]

        def issue_tasks(op, my_tasks):
            if len(my_tasks) == 0:
                return 0, 0, 0

            batches = op.form_batches(my_tasks)
    
            if self.cycle_stop.is_set():
                return 0, 0, 0

            nb = 0
            ns = 0
            nf = 0
   
            LOG.debug('Issuing transfer tasks.')
            for batch_tasks in batches:
                s, f = self._start_transfers(op, batch_tasks)
                nb += 1
                ns += s
                nf += f

                pending_count[op] += s
                if pending_count[op] > op.max_pending_transfers:
                    break

                if self.cycle_stop.is_set():
                    break

            return nb, ns, nf

        num_success = 0
        num_failure = 0
        num_batches = 0

        for condition, op in self.transfer_operations:
            if condition is None:
                default_op = op
                continue
            
            my_tasks = []
            for site in by_dest.keys():
                if condition.match(site):
                    my_tasks.extend(by_dest.pop(site))

            if pending_count[op] > op.max_pending_transfers:
                continue

            nb, ns, nf = issue_tasks(op, my_tasks)
            num_batches += nb
            num_success += ns
            num_failure += nf

            if self.cycle_stop.is_set():
                break

        else:
            # default condition
            if pending_count[default_op] <= default_op.max_pending_transfers:
                my_tasks = sum(by_dest.itervalues(), [])
                nb, ns, nf = issue_tasks(default_op, my_tasks)
                num_batches += nb
                num_success += ns
                num_failure += nf


        if num_success + num_failure != 0:
            LOG.info('Issued transfer tasks: %d success, %d failure. %d batches.', num_success, num_failure, num_batches)
        else:
            LOG.debug('Issued transfer tasks: %d success, %d failure. %d batches.', num_success, num_failure, num_batches)

    def delete_files(self, inventory):
        """
        Routine for managing file deletions.
        1. Query the file deletion agent and update the status of known subscriptions.
        2. Register the paths for completed deletions as candidates of empty directories.
        3. Pick up new subscriptions.
        4. Organize the deletions into batches.
        5. Start the deletions.
        The routine can be stopped almost at any point without corrupting the state machine.
        The only cases where stopping is problematic are within self._update_status and self._start_deletions.
        @param inventory   The inventory.
        """

        self._cleanup()

        LOG.debug('Clearing cancelled deletion tasks.')
        task_ids = self._get_cancelled_tasks('deletion')
        for _, op in self.deletion_operations:
            op.cancel_deletions(task_ids)

        if self.cycle_stop.is_set():
            return

        LOG.debug('Fetching deletion status from the file operation agent.')
        completed = self._update_status('deletion', inventory)

        LOG.debug('Recording candidates for empty directories.')
        self._set_dirclean_candidates(completed, inventory)

        if self.cycle_stop.is_set():
            return

        LOG.debug('Filtering out transfers to unavailable destinations.')
        if not self._read_only:
            for site in self.sites_in_downtime:
                self.db.query('UPDATE `file_subscriptions` SET `status` = \'held\', `hold_reason` = \'site_unavailable\' WHERE `site_id` = (SELECT `id` FROM `sites` WHERE `name` = %s)', site.name)

        if self.cycle_stop.is_set():
            return

        LOG.debug('Collecting new deletion subscriptions.')
        desubscriptions = self.get_subscriptions(inventory, op = 'deletion', status = ['new', 'retry'])

        if self.cycle_stop.is_set():
            return

        # See transfer_files
        pending_count = {}
        n_available = 0
        for _, op in self.deletion_operations:
            pending_count[op] = op.num_pending_deletions()
            if pending_count[op] <= op.max_pending_deletions:
                n_available += 1

        if n_available == 0:
            LOG.info('No deletion operators are available at the moment.')
            return

        tasks = [RLFSM.DeletionTask(d) for d in desubscriptions]

        by_site = {}
        for task in tasks:
            try:
                by_site[task.desubscription.site].append(task)
            except KeyError:
                by_site[task.desubscription.site] = [task]

        LOG.debug('Organizing the deletions into batches.')

        def issue_tasks(op, my_tasks):
            if len(my_tasks) == 0:
                return 0, 0, 0

            batches = op.form_batches(my_tasks)
    
            if self.cycle_stop.is_set():
                return 0, 0, 0

            nb = 0
            ns = 0
            nf = 0
    
            LOG.debug('Issuing deletion tasks for %d batches.', len(batches))    
            for batch_tasks in batches:
                LOG.debug('Batch with %d tasks.', len(batch_tasks))
                s, f = self._start_deletions(op, batch_tasks)
                nb += 1
                ns += s
                nf += f

                pending_count[op] += s
                if pending_count[op] > op.max_pending_deletions:
                    break

                if self.cycle_stop.is_set():
                    break

            return nb, ns, nf

        num_success = 0
        num_failure = 0
        num_batches = 0

        for condition, op in self.deletion_operations:
            if condition is None:
                default_op = op
                continue

            my_tasks = []
            for site in by_site.keys():
                if condition.match(site):
                    my_tasks.extend(by_site.pop(site))

            if pending_count[op] > op.max_pending_deletions:
                continue

            nb, ns, nf = issue_tasks(op, my_tasks)
            num_batches += nb;
            num_success += ns;
            num_failure += nf;

            if self.cycle_stop.is_set():
                break

        else:
            # default condition
            if pending_count[default_op] <= default_op.max_pending_deletions:
                my_tasks = sum(by_site.itervalues(), [])
                nb, ns, nf = issue_tasks(default_op, my_tasks)
                num_batches += nb;
                num_success += ns;
                num_failure += nf;

        if num_success + num_failure != 0:
            LOG.info('Issued deletion tasks: %d success, %d failure. %d batches.', num_success, num_failure, num_batches)
        else:
            LOG.debug('Issued deletion tasks: %d success, %d failure. %d batches.', num_success, num_failure, num_batches)

    def subscribe_file(self, site, lfile):
        """
        Make a file subscription at a site.
        @param site  Site object
        @param lfile File object
        """
        LOG.debug('Subscribing %s to %s', lfile.lfn, site.name)

        self._subscribe(site, lfile, 0)

    def desubscribe_file(self, site, lfile):
        """
        Book deletion of a file at a site.
        @param site  Site object
        @param lfile File object
        """
        LOG.debug('Desubscribing %s from %s', lfile.lfn, site.name)

        self._subscribe(site, lfile, 1)

    def cancel_subscription(self, site = None, lfile = None, sub_id = None):
        sql = 'UPDATE `file_subscriptions` SET `status` = \'cancelled\' WHERE '

        if sub_id is None:
            if site is None or lfile is None:
                raise OperationalError('site and lfile must be non-None.')

            sql += '`file_id` = %s AND `site_id` = %s'
            if not self._read_only:
                self.db.query(sql, lfile.id, site.id)
        else:
            sql += '`id` = %s'
            if not self._read_only:
                self.db.query(sql, sub_id)

    def cancel_desubscription(self, site = None, lfile = None, sub_id = None):
        self.cancel_subscription(site = site, lfile = lfile, sub_id = sub_id)

    def convert_pre_subscriptions(self, inventory):
        sql = 'SELECT `id`, `file_name`, `site_name`, UNIX_TIMESTAMP(`created`), `delete` FROM `file_pre_subscriptions`'

        sids = []

        for sid, lfn, site_name, created, delete in self.db.query(sql):
            lfile = inventory.find_file(lfn)
            if lfile is None or lfile.id == 0:
                continue

            try:
                site = inventory.sites[site_name]
            except KeyError:
                continue

            if site.id == 0:
                continue

            sids.append(sid)

            self._subscribe(site, lfile, delete, created = created)

        if not self._read_only:
            self.db.lock_tables(write = ['file_pre_subscriptions'])
            self.db.delete_many('file_pre_subscriptions', 'id', sids)
            if self.db.query('SELECT COUNT(*) FROM `file_pre_subscriptions`')[0] == 0:
                self.db.query('ALTER TABLE `file_pre_subscriptions` AUTO_INCREMENT = 1')
            self.db.unlock_tables()

    def get_subscriptions(self, inventory, op = None, status = None):
        """
        Return a list containing Subscription and Desubscription objects ordered by the id.
        @param inventory   Dynamo inventory
        @param op          If set to 'transfer' or 'deletion', limit to the operation type.
        @param status      If not None, set to list of status strings to limit the query.
        """

        # First convert all pre-subscriptions
        self.convert_pre_subscriptions(inventory)

        subscriptions = []

        get_all = 'SELECT u.`id`, u.`status`, u.`delete`, f.`block_id`, f.`name`, s.`name`, u.`hold_reason`, u.`created`'
        get_all += ' FROM `file_subscriptions` AS u'
        get_all += ' INNER JOIN `files` AS f ON f.`id` = u.`file_id`'
        get_all += ' INNER JOIN `sites` AS s ON s.`id` = u.`site_id`'

        constraints = []
        if op == 'transfer':
            constraints.append('u.`delete` = 0')
        elif op == 'deletion':
            constraints.append('u.`delete` = 1')
        if status is not None:
            constraints.append('u.`status` IN ' + MySQL.stringify_sequence(status))

        if len(constraints) != 0:
            get_all += ' WHERE ' + ' AND '.join(constraints)

        get_all += ' ORDER BY s.`id`, f.`block_id`'

        get_tried_sites = 'SELECT s.`name`, f.`exitcode` FROM `failed_transfers` AS f'
        get_tried_sites += ' INNER JOIN `sites` AS s ON s.`id` = f.`source_id`'
        get_tried_sites += ' WHERE f.`subscription_id` = %s'

        _destination_name = ''
        _block_id = -1

        no_source = []
        all_failed = []
        to_done = []

        COPY = 0
        DELETE = 1

        now_time = int(time.time())

        for row in self.db.query(get_all):
            sub_id, st, optype, block_id, file_name, site_name, hold_reason, created = row

            if site_name != _destination_name:
                _destination_name = site_name
                try:
                    destination = inventory.sites[site_name]
                except KeyError:
                    # Site disappeared from the inventory - weird but can happen!
                    destination = None

                _block_id = -1

            if destination is None:
                continue

            if block_id != _block_id:
                lfile = inventory.find_file(file_name)
                if lfile is None:
                    # Dataset, block, or file was deleted from the inventory earlier in this process 
                    #(deletion not reflected in the inventory store yet)
                    continue

                _block_id = block_id
                block = lfile.block
                dest_replica = block.find_replica(destination)

            else:
                lfile = block.find_file(file_name)
                if lfile is None:
                    # Dataset, block, or file was deleted from the inventory earlier in this process 
                    #(deletion not reflected in the inventory store yet)
                    continue

            if dest_replica is None and st != 'cancelled':
                LOG.debug('Destination replica for %s does not exist. Canceling the subscription.', file_name)
                # Replica was invalidated
                sql = 'UPDATE `file_subscriptions` SET `status` = \'cancelled\''
                sql += ' WHERE `id` = %s'
                if not self._read_only:
                    self.db.query(sql, sub_id)

                if status is not None and 'cancelled' not in status:
                    # We are not asked to return cancelled subscriptions
                    continue

                st = 'cancelled'

            if optype == COPY:
                disk_sources = None
                tape_sources = None
                failed_sources = None

                
                #create_time = int( time.mktime(datetime.datetime(created).timetuple()) )
                #if (now_time-create_time) > 7*(60*60*24):
                #    LOG.info('---- very old request-----')
                LOG.info(row)


                if st not in ('done', 'held', 'cancelled'):
                    if dest_replica.has_file(lfile):
                        LOG.debug('%s already exists at %s', file_name, site_name)
                        to_done.append(sub_id)

                        st = 'done'

                    else:
                        disk_sources = []
                        tape_sources = []
                        skip_rest = False
                        for replica in block.replicas:
                            if replica.site == destination or replica.site.status != Site.STAT_READY:
                                continue
            
                            if replica.has_file(lfile):
                                if replica.site.storage_type == Site.TYPE_DISK:
                                    disk_sources.append(replica.site)
                                elif replica.site.storage_type == Site.TYPE_MSS:
                                    tape_sources.append(replica.site)
                        if skip_rest:
                            continue
            
                        if len(disk_sources) + len(tape_sources) == 0:
                            LOG.info('Transfer of %s to %s has no source.', file_name, site_name)
                            no_source.append(sub_id)

                            st = 'held'

                if st == 'retry':
                    failed_sources = {}
                    for source_name, exitcode in self.db.query(get_tried_sites, sub_id):
                        try:
                            source = inventory.sites[source_name]
                        except KeyError:
                            # this site may have been deleted in this process
                            continue

                        try:
                            failed_sources[source].append(exitcode)
                        except KeyError:
                            if source not in disk_sources and source not in tape_sources:
                                # this is not a source site any more
                                continue

                            failed_sources[source] = [exitcode]
    
                    if len(failed_sources) == len(disk_sources) + len(tape_sources):
                        # transfers from all sites failed at least once
                        for codes in failed_sources.itervalues():
                            if codes[-1] not in irrecoverable_errors:
                                # This site failed for a recoverable reason
                                break
                        else:
                            LOG.info('Number of disk sources: '+ str(len(disk_sources)))
                            LOG.info('Number of tape sources: '+ str(len(tape_sources)))
                            # last failure from all sites due to irrecoverable errors
                            LOG.warning('Transfer of %s to %s failed from all sites.', file_name, site_name)
                            all_failed.append(sub_id)

                            st = 'held'

                # st value may have changed - filter again
                if status is None or st in status:
                    subscription = RLFSM.Subscription(sub_id, st, lfile, destination, disk_sources, tape_sources, failed_sources, hold_reason)
                    subscriptions.append(subscription)

            elif optype == DELETE:
                if st not in ('done', 'held', 'cancelled') and not dest_replica.has_file(lfile):
                    LOG.debug('%s is already gone from %s', file_name, site_name)
                    to_done.append(sub_id)

                    st = 'done'

                if status is None or st in status:
                    desubscription = RLFSM.Desubscription(sub_id, st, lfile, destination)
                    subscriptions.append(desubscription)

        if len(to_done) + len(no_source) + len(all_failed) != 0:
            msg = 'Subscriptions terminated directly: %d done' % len(to_done)
            if len(no_source) != 0:
                msg += ', %d held with reason "no_source"' % len(no_source)
            if len(all_failed) != 0:
                msg += ', %d held with reason "all_failed"' % len(all_failed)

        if not self._read_only:
            self.db.execute_many('UPDATE `file_subscriptions` SET `status` = \'done\', `last_update` = NOW()', 'id', to_done)
            self.db.execute_many('UPDATE `file_subscriptions` SET `status` = \'held\', `hold_reason` = \'no_source\', `last_update` = NOW()', 'id', no_source)
            self.db.execute_many('UPDATE `file_subscriptions` SET `status` = \'held\', `hold_reason` = \'all_failed\', `last_update` = NOW()', 'id', all_failed)

            # Clean up subscriptions for deleted files / sites
            sql = 'DELETE FROM u USING `file_subscriptions` AS u'
            sql += ' LEFT JOIN `files` AS f ON f.`id` = u.`file_id`'
            sql += ' LEFT JOIN `sites` AS s ON s.`id` = u.`site_id`'
            sql += ' WHERE f.`name` IS NULL OR s.`name` IS NULL'
            self.db.query(sql)

            sql = 'DELETE FROM f USING `failed_transfers` AS f'
            sql += ' LEFT JOIN `file_subscriptions` AS u ON u.`id` = f.`subscription_id`'
            sql += ' WHERE u.`id` IS NULL'
            self.db.query(sql)

        return subscriptions

    def close_subscriptions(self, done_ids):
        """
        Get subscription completion acknowledgments.
        """

        if not self._read_only:
            self.db.delete_many('file_subscriptions', 'id', done_ids)

    def release_subscription(self, subscription):
        """
        Clear failed transfers list and set the subscription status to retry.
        """

        if subscription.status != 'held':
            return

        if self._read_only:
            return

        self.db.query('DELETE FROM `failed_transfers` WHERE `subscription_id` = %s', subscription.id)
        self.db.query('UPDATE `file_subscriptions` SET `status` = \'new\' WHERE `id` = %s', subscription.id)

    def _run_cycle(self, inventory):
        while True:
            if self.cycle_stop.is_set():
                break
    
            LOG.debug('Checking and executing new file transfer subscriptions.')
            self.transfer_files(inventory)
    
            if self.cycle_stop.is_set():
                break
    
            LOG.debug('Checking and executing new file deletion subscriptions.')
            self.delete_files(inventory)

            is_set = self.cycle_stop.wait(30)
            if is_set: # is true if in Python 2.7 and the flag is set
                break

    def _cleanup(self):
        if self._read_only:
            return

        # Make the tables consistent in case the previous cycles was terminated prematurely

        # There should not be tasks with subscription status new
        sql = 'DELETE FROM t USING `transfer_tasks` AS t'
        sql += ' INNER JOIN `file_subscriptions` AS u ON u.`id` = t.`subscription_id`'
        sql += ' WHERE u.`status` IN (\'new\', \'retry\')'
        self.db.query(sql)
        sql = 'DELETE FROM t USING `deletion_tasks` AS t'
        sql += ' INNER JOIN `file_subscriptions` AS u ON u.`id` = t.`subscription_id`'
        sql += ' WHERE u.`status` IN (\'new\', \'retry\')'
        self.db.query(sql)

        # There should not be batches with no tasks
        sql = 'DELETE FROM b USING `transfer_batches` AS b LEFT JOIN `transfer_tasks` AS t ON t.`batch_id` = b.`id` WHERE t.`batch_id` IS NULL'
        self.db.query(sql)
        sql = 'DELETE FROM b USING `deletion_batches` AS b LEFT JOIN `deletion_tasks` AS t ON t.`batch_id` = b.`id` WHERE t.`batch_id` IS NULL'
        self.db.query(sql)

        # and tasks with no batches
        sql = 'DELETE FROM t USING `transfer_tasks` AS t LEFT JOIN `transfer_batches` AS b ON b.`id` = t.`batch_id` WHERE b.`id` IS NULL'
        self.db.query(sql)
        sql = 'DELETE FROM t USING `deletion_tasks` AS t LEFT JOIN `deletion_batches` AS b ON b.`id` = t.`batch_id` WHERE b.`id` IS NULL'
        self.db.query(sql)

        # Cleanup the plugins (might delete tasks)
        for _, op in self.transfer_operations:
            op.cleanup()
        if self.deletion_operations is not self.transfer_operations:
            for _, op in self.deletion_operations:
                op.cleanup()

        # Reset inbatch subscriptions with no task to new state
        sql = 'UPDATE `file_subscriptions` SET `status` = \'new\' WHERE `status` = \'inbatch\' AND `id` NOT IN (SELECT `subscription_id` FROM `transfer_tasks`) AND `id` NOT IN (SELECT `subscription_id` FROM `deletion_tasks`)'
        self.db.query(sql)

        # Delete canceled subscriptions with no task (ones with task need to be archived in update_status)
        sql = 'DELETE FROM u USING `file_subscriptions` AS u LEFT JOIN `transfer_tasks` AS t ON t.`subscription_id` = u.`id` WHERE u.`delete` = 0 AND u.`status` = \'cancelled\' AND t.`id` IS NULL'
        self.db.query(sql)

        sql = 'DELETE FROM u USING `file_subscriptions` AS u LEFT JOIN `deletion_tasks` AS t ON t.`subscription_id` = u.`id` WHERE u.`delete` = 1 AND u.`status` = \'cancelled\' AND t.`id` IS NULL'
        self.db.query(sql)

        # Delete failed transfers with no subscription
        sql = 'DELETE FROM f USING `failed_transfers` AS f LEFT JOIN `file_subscriptions` AS u ON u.`id` = f.`subscription_id` WHERE u.`id` IS NULL'
        self.db.query(sql)

    def _subscribe(self, site, lfile, delete, created = None):
        opp_op = 0 if delete == 1 else 1
        now = time.strftime('%Y-%m-%d %H:%M:%S')

        if created is None:
            created = now
        else:
            created = datetime.datetime(*time.localtime(created)[:6])

        if lfile.id == 0 or site.id == 0:
            # file is not registered in inventory store yet; update the presubscription
            if not self._read_only:
                fields = ('file_name', 'site_name', 'created', 'delete')
                self.db.insert_update('file_pre_subscriptions', fields, lfile.lfn, site.name, now, delete, update_columns = ('delete',))
            return

        if not self._read_only:
            self.db.lock_tables(write = ['file_subscriptions'])

        try:
            sql = 'UPDATE `file_subscriptions` SET `status` = \'cancelled\''
            sql += ' WHERE `file_id` = %s AND `site_id` = %s AND `delete` = %s'
            sql += ' AND `status` IN (\'new\', \'inbatch\', \'retry\', \'held\')'
            if not self._read_only:
                self.db.query(sql, lfile.id, site.id, opp_op)
    
            fields = ('file_id', 'site_id', 'status', 'delete', 'created', 'last_update')

            if not self._read_only:
                self.db.insert_update('file_subscriptions', fields, lfile.id, site.id, 'new', delete, now, now, update_columns = ('status', 'last_update'))

        finally:
            if not self._read_only:
                self.db.unlock_tables()

    def _get_cancelled_tasks(self, optype):
        if optype == 'transfer':
            delete = 0
        else:
            delete = 1

        sql = 'SELECT q.`id` FROM `{op}_tasks` AS q'.format(op = optype)
        sql += ' INNER JOIN `file_subscriptions` AS u ON u.`id` = q.`subscription_id`'
        sql += ' WHERE u.`status` = \'cancelled\' AND u.`delete` = %d' % delete
        return self.db.query(sql)

    def _update_status(self, optype, inventory):
        if optype == 'transfer':
            site_columns = 'ss.`name`, sd.`name`'
            site_joins = ' INNER JOIN `sites` AS ss ON ss.`id` = q.`source_id`'
            site_joins += ' INNER JOIN `sites` AS sd ON sd.`id` = u.`site_id`'
        else:
            site_columns = 's.`name`'
            site_joins = ' INNER JOIN `sites` AS s ON s.`id` = u.`site_id`'

        get_task_data = 'SELECT u.`id`, f.`name`, f.`size`, UNIX_TIMESTAMP(q.`created`), ' + site_columns + ' FROM `{op}_tasks` AS q'
        get_task_data += ' INNER JOIN `file_subscriptions` AS u ON u.`id` = q.`subscription_id`'
        get_task_data += ' INNER JOIN `files` AS f ON f.`id` = u.`file_id`'
        get_task_data += site_joins
        get_task_data += ' WHERE q.`id` = %s'

        get_task_data = get_task_data.format(op = optype)

        if optype == 'transfer':
            history_table_name = 'file_transfers'
            history_site_fields = ('source_id', 'destination_id')
        else:
            history_table_name = 'file_deletions'
            history_site_fields = ('site_id',)

        history_fields = ('file_id', 'exitcode', 'message', 'batch_id', 'created', 'started', 'finished', 'completed') + history_site_fields

        if optype == 'transfer':
            insert_failure = 'INSERT INTO `failed_transfers` (`id`, `subscription_id`, `source_id`, `exitcode`)'
            insert_failure += ' SELECT `id`, `subscription_id`, `source_id`, %s FROM `transfer_tasks` WHERE `id` = %s'
            insert_failure += ' ON DUPLICATE KEY UPDATE `id`=VALUES(`id`)'
            delete_failures = 'DELETE FROM `failed_transfers` WHERE `subscription_id` = %s'

        get_subscription_status = 'SELECT `status` FROM `file_subscriptions` WHERE `id` = %s'

        update_subscription = 'UPDATE `file_subscriptions` SET `status` = %s, `last_update` = NOW() WHERE `id` = %s'
        delete_subscription = 'DELETE FROM `file_subscriptions` WHERE `id` = %s'

        delete_task = 'DELETE FROM `{op}_tasks` WHERE `id` = %s'.format(op = optype)

        delete_batch = 'DELETE FROM `{op}_batches` WHERE `id` = %s'.format(op = optype)

        done_subscriptions = []
        num_success = 0
        num_failure = 0
        num_cancelled = 0

        # Collect completed tasks

        total_counter = 0

        for batch_id in self.db.query('SELECT `id` FROM `{op}_batches`'.format(op = optype)):
            results = []

            
            if optype == 'transfer':

                total_counter = total_counter + 1

#                if total_counter > 200:
#                    LOG.info('-- getiing out transfer_status as counter > ' + str(total_counter))
#                    break

                
                for condition, query in self.transfer_queries:
                    results = query.get_transfer_status(batch_id)
                    if len(results) != 0:
                        break

            else:
                for condition, query in self.deletion_queries:
                    results = query.get_deletion_status(batch_id)
                    if len(results) != 0:
                        break


            batch_complete = True

            for task_id, status, exitcode, message, start_time, finish_time in results:
                # start_time and finish_time can be None
                LOG.debug('%s result: %d %s %d %s %s', optype, task_id, FileQuery.status_name(status), exitcode, start_time, finish_time)

                if status == FileQuery.STAT_DONE:
                    num_success += 1
                elif status == FileQuery.STAT_FAILED:
                    num_failure += 1
                elif status == FileQuery.STAT_CANCELLED:
                    num_cancelled += 1
                else:
                    batch_complete = False
                    continue

                try:
                    task_data = self.db.query(get_task_data, task_id)[0]
                except IndexError:
                    LOG.warning('%s task %d got lost.', optype, task_id)
                    if optype == 'transfer':
                        query.forget_transfer_status(task_id)
                    else:
                        query.forget_deletion_status(task_id)

                    if not self._read_only:
                        self.db.query(delete_task, task_id)

                    continue

                subscription_id, lfn, size, create_time = task_data[:4]

                if optype == 'transfer':
                    source_name, dest_name = task_data[4:]
                    history_site_ids = (
                        self.history_db.save_sites([source_name], get_ids = True)[0],
                        self.history_db.save_sites([dest_name], get_ids = True)[0]
                    )
                else:
                    site_name = task_data[4]
                    history_site_ids = (self.history_db.save_sites([site_name], get_ids = True)[0],)

                file_id = self.history_db.save_files([(lfn, size)], get_ids = True)[0]

                if start_time is None:
                    sql_start_time = None
                else:
                    sql_start_time = datetime.datetime(*time.localtime(start_time)[:6])

                if finish_time is None:
                    sql_finish_time = None
                else:
                    sql_finish_time = datetime.datetime(*time.localtime(finish_time)[:6])

                values = (file_id, exitcode, message, batch_id, datetime.datetime(*time.localtime(create_time)[:6]),
                    sql_start_time, sql_finish_time, MySQL.bare('NOW()')) + history_site_ids

                if optype == 'transfer':
                    LOG.debug('Archiving transfer of %s from %s to %s (exitcode %d)', lfn, source_name, dest_name, exitcode)
                else:
                    LOG.debug('Archiving deletion of %s at %s (exitcode %d)', lfn, site_name, exitcode)

                if self._read_only:
                    history_id = 0
                else:
                    #LOG.error(values)
                    #values_tmp = set()
                    #for v in values:
                    #    try:
                    #        tmp = v.replace(u"\u2019", "'")
                    #        values_tmp.add(tmp)
                    #    except:
                    #        values_tmp.add(v)
                    #LOG.error(history_fields)
                    #LOG.error(values_tmp)
                    #values = values_tmp

                    history_id = self.history_db.db.insert_get_id(history_table_name, history_fields, values)

                if optype == 'transfer':
                    query.write_transfer_history(self.history_db, task_id, history_id)
                else:
                    query.write_deletion_history(self.history_db, task_id, history_id)

                # We check the subscription status and update accordingly. Need to lock the tables.
                if not self._read_only:
                    self.db.lock_tables(write = ['file_subscriptions'])

                try:
                    subscription_status = self.db.query(get_subscription_status, subscription_id)[0]

                    if subscription_status == 'inbatch':
                        if status == FileQuery.STAT_DONE:
                            LOG.debug('Subscription %d done.', subscription_id)
                            if not self._read_only:
                                self.db.query(update_subscription, 'done', subscription_id)
        
                        elif status == FileQuery.STAT_FAILED:
                            LOG.debug('Subscription %d failed (exit code %d). Flagging retry.', subscription_id, exitcode)
                            if not self._read_only:
                                self.db.query(update_subscription, 'retry', subscription_id)
        
                    elif subscription_status == 'cancelled':
                        # subscription is cancelled and task terminated -> delete the subscription now, irrespective of the task status
                        LOG.debug('Subscription %d is cancelled.', subscription_id)
                        if not self._read_only:
                            self.db.query(delete_subscription, subscription_id)
                finally:
                    if not self._read_only:
                        self.db.unlock_tables()

                if not self._read_only:
                    if optype == 'transfer':
                        if subscription_status == 'cancelled' or (subscription_status == 'inbatch' and status == FileQuery.STAT_DONE):
                            # Delete entries from failed_transfers table
                            self.db.query(delete_failures, subscription_id)
    
                        elif subscription_status == 'inbatch' and status == FileQuery.STAT_FAILED:
                            # Insert entry to failed_transfers table
                            self.db.query(insert_failure, exitcode, task_id)
        
                    self.db.query(delete_task, task_id)

                if status == FileQuery.STAT_DONE:
                    done_subscriptions.append(subscription_id)

                if optype == 'transfer':
                    query.forget_transfer_status(task_id)
                else:
                    query.forget_deletion_status(task_id)

                if self.cycle_stop.is_set():
                    break

            if batch_complete:
                if not self._read_only:
                    self.db.query(delete_batch, batch_id)

                if optype == 'transfer':
                    query.forget_transfer_batch(batch_id)
                else:
                    query.forget_deletion_batch(batch_id)

        if num_success + num_failure + num_cancelled != 0:
            LOG.info('Archived file %s: %d succeeded, %d failed, %d cancelled.', optype, num_success, num_failure, num_cancelled)
        else:
            LOG.debug('Archived file %s: %d succeeded, %d failed, %d cancelled.', optype, num_success, num_failure, num_cancelled)

        return done_subscriptions

    def _select_source(self, subscriptions):
        """
        Intelligently select the best source for each subscription.
        @param subscriptions  List of Subscription objects

        @return  List of TransferTask objects
        """

        def find_site_to_try(sources, failed_sources):
            not_tried = set(sources)
            if failed_sources is not None:
                not_tried -= set(failed_sources.iterkeys())

            LOG.debug('%d sites not tried', len(not_tried))

            if len(not_tried) == 0:
                if failed_sources is None:
                    return None

                # we've tried all sites. Did any of them fail with a recoverable error?
                sites_to_retry = []
                for site, codes in failed_sources.iteritems():
                    if site not in sources:
                        continue

                    if codes[-1] not in irrecoverable_errors:
                        sites_to_retry.append(site)

                if len(sites_to_retry) == 0:
                    return None
                else:
                    # select the least failed site
                    by_failure = sorted(sites_to_retry, key = lambda s: len(failed_sources[s]))
                    LOG.debug('%s has the least failures', by_failure[0].name)
                    return by_failure[0]

            else:
                LOG.debug('Selecting randomly')
                return random.choice(list(not_tried))

        tasks = []

        for subscription in subscriptions:
            LOG.debug('Selecting a disk source for subscription %d (%s to %s)', subscription.id, subscription.file.lfn, subscription.destination.name)
            source = find_site_to_try(subscription.disk_sources, subscription.failed_sources)
            if source is None:
                LOG.debug('Selecting a tape source for subscription %d', subscription.id)
                source = find_site_to_try(subscription.tape_sources, subscription.failed_sources)

            if source is None:
                # If both disk and tape failed irrecoveably, the subscription must be placed in held queue in get_subscriptions.
                # Reaching this line means something is wrong.
                LOG.warning('Could not find a source for transfer of %s to %s from %d disk and %d tape candidates.',
                    subscription.file.lfn, subscription.destination.name, len(subscription.disk_sources), len(subscription.tape_sources))
                continue
            
            tasks.append(RLFSM.TransferTask(subscription, source))

        return tasks

    def _start_transfers(self, transfer_operation, tasks):
        # start the transfer of tasks. If batch submission fails, make progressively smaller batches until failing tasks are identified.
        if self._read_only:
            batch_id = 0
        else:
            self.db.query('INSERT INTO `transfer_batches` (`id`) VALUES (0)')
            batch_id = self.db.last_insert_id

        LOG.debug('New transfer batch %d for %d files.', batch_id, len(tasks))

        # local time
        now = time.strftime('%Y-%m-%d %H:%M:%S')

        # need to create the transfer tasks first to have ids assigned
        fields = ('subscription_id', 'source_id', 'batch_id', 'created')
        mapping = lambda t: (t.subscription.id, t.source.id, batch_id, now)

        if not self._read_only:
            self.db.insert_many('transfer_tasks', fields, mapping, tasks)
        
        # set the task ids
        tasks_by_sub = dict((t.subscription.id, t) for t in tasks)
        for task_id, subscription_id in self.db.xquery('SELECT `id`, `subscription_id` FROM `transfer_tasks` WHERE `batch_id` = %s', batch_id):
            tasks_by_sub[subscription_id].id = task_id

        result = transfer_operation.start_transfers(batch_id, tasks)

        successful = [task for task, success in result.iteritems() if success]

        if not self._read_only:
            self.db.execute_many('UPDATE `file_subscriptions` SET `status` = \'inbatch\', `last_update` = NOW()', 'id', [t.subscription.id for t in successful])

            if len(successful) != len(result):
                failed = [task for task, success in result.iteritems() if not success]
                for task in failed:
                    LOG.error('Cannot issue transfer of %s from %s to %s',
                              task.subscription.file.lfn, task.source.name, task.subscription.destination.name)

                failed_ids = [t.id for t in failed]

                sql = 'INSERT INTO `failed_transfers` (`id`, `subscription_id`, `source_id`, `exitcode`)'
                sql += ' SELECT `id`, `subscription_id`, `source_id`, -1 FROM `transfer_tasks`'
                self.db.execute_many(sql, 'id', failed_ids)

                self.db.delete_many('transfer_tasks', 'id', failed_ids)

                self.db.execute_many('UPDATE `file_subscriptions` SET `status` = \'retry\', `last_update` = NOW()', 'id', [t.subscription.id for t in failed])

        return len(successful), len(result) - len(successful)

    def _start_deletions(self, deletion_operation, tasks):
        if self._read_only:
            batch_id = 0
        else:
            self.db.query('INSERT INTO `deletion_batches` (`id`) VALUES (0)')
            batch_id = self.db.last_insert_id

        # local time
        now = time.strftime('%Y-%m-%d %H:%M:%S')
        
        fields = ('subscription_id', 'batch_id', 'created')
        mapping = lambda t: (t.desubscription.id, batch_id, now)

        if not self._read_only:
            self.db.insert_many('deletion_tasks', fields, mapping, tasks)

        # set the task ids
        tasks_by_sub = dict((t.desubscription.id, t) for t in tasks)
        for task_id, desubscription_id in self.db.xquery('SELECT `id`, `subscription_id` FROM `deletion_tasks` WHERE `batch_id` = %s', batch_id):
            tasks_by_sub[desubscription_id].id = task_id
        
        result = deletion_operation.start_deletions(batch_id, tasks)

        successful = [task for task, success in result.iteritems() if success]

        if not self._read_only:
            self.db.execute_many('UPDATE `file_subscriptions` SET `status` = \'inbatch\', `last_update` = NOW()', 'id', [t.desubscription.id for t in successful])

            if len(successful) != len(result):
                failed = [task for task, success in result.iteritems() if not success]

                for task in failed:
                    LOG.error('Cannot delete %s at %s',
                              task.desubscription.file.lfn, task.desubscription.site.name)

                self.db.delete_many('deletion_tasks', 'id', [t.id for t in failed])

                self.db.execute_many('UPDATE `file_subscriptions` SET `status` = \'held\', `last_update` = NOW()', 'id', [t.desubscription.id for t in failed])

        return len(successful), len(result) - len(successful)
    
    def _set_dirclean_candidates(self, subscription_ids, inventory):
        site_dirs = {}

        # Clean up directories of completed subscriptions
        sql = 'SELECT s.`name`, f.`name` FROM `file_subscriptions` AS u'
        sql += ' INNER JOIN `files` AS f ON f.`id` = u.`file_id`'
        sql += ' INNER JOIN `sites` AS s ON s.`id` = u.`site_id`'

        for site_name, file_name in self.db.execute_many(sql, 'u.`id`', subscription_ids):
            try:
                site = inventory.sites[site_name]
            except KeyError:
                continue

            try:
                dirs = site_dirs[site]
            except KeyError:
                dirs = site_dirs[site] = set()

            dirs.add(os.path.dirname(file_name))

        def get_entry():
            for site, dirs in site_dirs.iteritems():
                for directory in dirs:
                    yield site.id, directory

        fields = ('site_id', 'directory')
        if not self._read_only:
            self.db.insert_many('directory_cleaning_tasks', fields, None, get_entry(), do_update = True)
