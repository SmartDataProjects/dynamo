import os
import collections
import random
import time
import threading
import logging

from dynamo.fileop.base import FileQuery
from dynamo.fileop.transfer import FileTransferOperation, FileTransferQuery
from dynamo.fileop.deletion import FileDeletionOperation, FileDeletionQuery, DirDeletionOperation
from dynamo.dataformat import Configuration, Block, Site, BlockReplica
from dynamo.utils.interface.mysql import MySQL

LOG = logging.getLogger(__name__)

class RLFSM(object):
    """
    File operations manager using MySQL tables for queue bookkeeping. Also implies the
    inventory backend is MySQL.
    """

    class Subscription(object):
        __slots__ = ['id', 'status', 'file', 'destination', 'disk_sources', 'tape_sources', 'failed_sources']

        def __init__(self, id, status, file, destination, disk_sources, tape_sources, failed_sources = None):
            self.id = id
            self.status = status
            self.file = file
            self.destination = destination
            self.disk_sources = disk_sources
            self.tape_sources = tape_sources
            self.failed_sources = failed_sources

    class TransferTask(object):
        __slots__ = ['id', 'subscription', 'source']

        def __init__(self, subscription, source):
            self.id = None
            self.subscription = subscription
            self.source = source

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

    # exit codes
    TR_NO_FILE = 2

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

        # History DB name
        self.history_db = config.db.history

        # FileTransferOperation backend (can make it a map from (source, dest) to operator)
        if 'transfer' in config:
            self.transfer_operation = FileTransferOperation.get_instance(config.transfer.module, config.transfer.config)
        else:
            # if all you need to do is make subscriptions
            self.transfer_operation = None

        # QueryOperation backend
        if 'transfer_query' in config:
            self.transfer_query = FileTransferQuery.get_instance(config.transfer_query.module, config.transfer_query.config)
        else:
            self.transfer_query = self.transfer_operation

        # FileDeletionOperation backend (can make it a map from dest to operator)
        if 'deletion' in config:
            self.deletion_operation = FileDeletionOperation.get_instance(config.deletion.module, config.deletion.config)
        else:
            self.deletion_operation = self.transfer_operation

        # QueryOperation backend
        if 'deletion_query' in config:
            self.deletion_query = FileDeletionQuery.get_instance(config.deletion_query.module, config.deletion_query.config)
        else:
            self.deletion_query = self.deletion_operation

        # Cycle thread
        self.main_cycle = None
        self.cycle_stop = threading.Event()

        self.set_read_only(config.get('read_only', False))

    def set_read_only(self, value = True):
        self._read_only = value
        if self.transfer_operation:
            self.transfer_operation.set_read_only(value)
        if self.deletion_operation:
            self.deletion_operation.set_read_only(value)

    def start(self, inventory):
        """
        Start the file operations management cycle. Issue transfer and deletion tasks to the backend.
        """

        if self.main_cycle is not None:
            return

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

        LOG.debug('Clearing cancelled transfer tasks.')
        task_ids = self._get_cancelled_tasks('transfer')
        self.transfer_operation.cancel_transfers(task_ids)

        if self.cycle_stop.is_set():
            return

        LOG.debug('Fetching subscription status from the file operation agent.')
        self._update_status('transfer')

        if self.cycle_stop.is_set():
            return

        LOG.debug('Collecting new transfer subscriptions.')
        subscriptions = self.get_subscriptions(inventory, op = 'transfer', status = ['new', 'retry'])

        if self.cycle_stop.is_set():
            return

        LOG.debug('Identifying source sites for %d transfers.', len(subscriptions))
        tasks = self._select_source(subscriptions)

        if self.cycle_stop.is_set():
            return

        LOG.debug('Organizing %d transfers into batches.', len(tasks))
        batches = self.transfer_operation.form_batches(tasks)

        if self.cycle_stop.is_set():
            return

        LOG.debug('Issuing transfer tasks.')
        num_success = 0
        num_failure = 0
        for batch_tasks in batches:
            s, f = self._start_transfers(batch_tasks)
            num_success += s
            num_failure += f
            if self.cycle_stop.is_set():
                break

        if len(batches):
            LOG.info('Issued transfer tasks: %d success, %d failure. %d batches.', num_success, num_failure, len(batches))
        else:
            LOG.debug('Issued transfer tasks: %d success, %d failure. %d batches.', num_success, num_failure, len(batches))

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

        LOG.debug('Clearing cancelled deletion tasks.')
        task_ids = self._get_cancelled_tasks('deletion')
        self.deletion_operation.cancel_deletions(task_ids)

        if self.cycle_stop.is_set():
            return

        LOG.debug('Fetching deletion status from the file operation agent.')
        completed = self._update_status('deletion')

        LOG.debug('Recording candidates for empty directories.')
        self._set_dirclean_candidates(completed, inventory)

        if self.cycle_stop.is_set():
            return

        LOG.debug('Collecting new deletion subscriptions.')
        desubscriptions = self.get_subscriptions(inventory, op = 'deletion', status = ['new', 'retry'])

        if self.cycle_stop.is_set():
            return

        tasks = [RLFSM.DeletionTask(d) for d in desubscriptions]

        LOG.debug('Organizing the deletions into batches.')
        batches = self.deletion_operation.form_batches(tasks)

        if self.cycle_stop.is_set():
            return

        LOG.debug('Issuing deletion tasks.')
        num_success = 0
        num_failure = 0
        for batch_tasks in batches:
            s, f = self._start_deletions(batch_tasks)
            num_success += s
            num_failure += f
            if self.cycle_stop.is_set():
                break
    
        if len(batches) != 0:
            LOG.info('Issued deletion tasks: %d success, %d failure. %d batches.', num_success, num_failure, len(batches))
        else:
            LOG.debug('Issued deletion tasks: %d success, %d failure. %d batches.', num_success, num_failure, len(batches))

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

        get_all = 'SELECT u.`id`, u.`status`, u.`delete`, f.`id`, f.`name`, s.`name` FROM `file_subscriptions` AS u'
        get_all += ' INNER JOIN `files` AS f ON f.`id` = u.`file_id`'
        get_all += ' INNER JOIN `sites` AS s ON s.`id` = u.`site_id`'

        constraints = []
        if op == 'transfer':
            constraints.append('`delete` = 0')
        elif op == 'deletion':
            constraints.append('`delete` = 1')
        if status is not None:
            constraints.append('u.`status` IN ' + MySQL.stringify_sequence(status))

        if len(constraints) != 0:
            get_all += ' WHERE ' + ' AND '.join(constraints)

        get_all += ' ORDER BY s.`id`'

        get_tried_sites = 'SELECT s.`name`, f.`exitcode` FROM `failed_transfers` AS f'
        get_tried_sites += ' INNER JOIN `sites` AS s ON s.`id` = f.`source_id`'
        get_tried_sites += ' WHERE f.`subscription_id` = %s'

        destination = None

        to_hold = []
        to_done = []

        COPY = 0
        DELETE = 1

        for row in self.db.query(get_all):
            sub_id, st, optype, file_id, file_name, site_name = row

            if destination is None or site_name != destination.name:
                try:
                    destination = inventory.sites[site_name]
                except KeyError:
                    # Site disappeared from the inventory - weird but can happen!
                    continue

            lfile = inventory.find_file(file_name)
            if lfile is None:
                # Dataset, block, or file was deleted from the inventory earlier in this process (deletion not reflected in the inventory store yet)
                continue

            dest_replica = lfile.block.find_replica(destination)
            if dest_replica is None and st != 'cancelled':
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
                if st not in ('done', 'held', 'cancelled'):
                    if dest_replica.has_file(lfile):
                        LOG.debug('%s already exists at %s', file_name, site_name)
                        to_done.append(sub_id)
                        continue

                    disk_sources = []
                    tape_sources = []
                    for replica in lfile.block.replicas:
                        if replica.site == destination or replica.site.status != Site.STAT_READY:
                            continue
        
                        if replica.file_ids is None or file_id in replica.file_ids:
                            if replica.site.storage_type == Site.TYPE_DISK:
                                disk_sources.append(replica.site)
                            elif replica.site.storage_type == Site.TYPE_MSS:
                                tape_sources.append(replica.site)
        
                    if len(disk_sources) + len(tape_sources) == 0:
                        LOG.warning('Transfer of %s to %s has no source.', file_name, site_name)
                        to_hold.append(sub_id)
                        # don't add this subscritpion to subscriptions list
                        continue
                else:
                    disk_sources = None
                    tape_sources = None
    
                if st == 'retry':
                    failed_sources = {}
                    for source_name, exitcode in self.db.query(get_tried_sites, sub_id):
                        try:
                            source = inventory.sites[source_name]
                        except KeyError:
                            # this site may have been deleted in this process
                            continue

                        if source not in failed_sources:
                            failed_sources[source] = [exitcode]
                        else:
                            failed_sources[source].append(exitcode)
    
                    if len(failed_sources) == len(disk_sources) + len(tape_sources):
                        # transfers from all sites failed at least once
                        for codes in failed_sources.itervalues():
                            if codes[-1] != RLFSM.TR_NO_FILE:
                                break
                        else:
                            # last failure from all sites due to missing file at source
                            to_hold.append(sub_id)
                            # don't add this subscritpion to subscriptions list
                            continue
                else:
                    failed_sources = None
    
                subscription = RLFSM.Subscription(sub_id, st, lfile, destination, disk_sources, tape_sources, failed_sources)
                subscriptions.append(subscription)

            elif optype == DELETE:
                if st not in ('done', 'held', 'cancelled') and not dest_replica.has_file(lfile):
                    LOG.debug('%s is already gone from %s', file_name, site_name)
                    to_done.append(sub_id)
                    continue

                desubscription = RLFSM.Desubscription(sub_id, st, lfile, destination)
                subscriptions.append(desubscription)

        if not self._read_only:
            self.db.execute_many('UPDATE `file_subscriptions` SET `status` = \'done\', `last_update` = NOW()', 'id', to_done)
            self.db.execute_many('UPDATE `file_subscriptions` SET `status` = \'held\', `last_update` = NOW()', 'id', to_hold)

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

    def _subscribe(self, site, lfile, delete, created = None):
        opp_op = 0 if delete == 1 else 1
        now = time.strftime('%Y-%m-%d %H:%M:%S')

        if created is None:
            created = now
        else:
            created = MySQL.bare('FROM_UNIXTIME(%d)' % created)

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

    def _update_status(self, optype):
        # insert queries all have ON DUPLICATE key to make sure we can restart in case of a crash

        insert_file = 'INSERT INTO `{history}`.`files` (`name`, `size`)'
        insert_file += ' SELECT f.`name`, f.`size` FROM `transfer_tasks` AS q'
        insert_file += ' INNER JOIN `file_subscriptions` AS u ON u.`id` = q.`subscription_id`'
        insert_file += ' INNER JOIN .`files` AS f ON f.`id` = u.`file_id`'
        insert_file += ' WHERE u.`delete` = 0 AND q.`id` = %s'
        insert_file += ' ON DUPLICATE KEY UPDATE `size`=VALUES(`size`)'

        insert_file = insert_file.format(history = self.history_db)

        # sites have to be inserted to history already

        if optype == 'transfer':
            table_name = 'file_transfers'
            site_fields = '`source_id`, `destination_id`'
            site_values = 'hss.`id`, hsd.`id`'
            site_joins = ' INNER JOIN `sites` AS sd ON sd.`id` = u.`site_id` INNER JOIN `sites` AS ss ON ss.`id` = q.`source_id`'
            site_joins += ' INNER JOIN `{history}`.`sites` AS hsd ON hsd.`name` = sd.`name` INNER JOIN `{history}`.`sites` AS hss ON hss.`name` = ss.`name`'
        else:
            table_name = 'file_deletions'
            site_fields = '`site_id`'
            site_values = 'hs.`id`'
            site_joins = ' INNER JOIN `sites` AS s ON s.`id` = u.`site_id`'
            site_joins += ' INNER JOIN `{history}`.`sites` AS hs ON hs.`name` = s.`name`'

        insert_history = 'INSERT INTO `{history}`.`{table}`'
        insert_history += ' (`file_id`, ' + site_fields + ', `exitcode`, `batch_id`, `created`, `started`, `finished`, `completed`)'
        insert_history += ' SELECT hf.`id`, ' + site_values + ', %s, q.`batch_id`, q.`created`, FROM_UNIXTIME(%s), FROM_UNIXTIME(%s), NOW()'
        insert_history += ' FROM `{op}_tasks` AS q'
        insert_history += ' INNER JOIN `file_subscriptions` AS u ON u.`id` = q.`subscription_id`'
        insert_history += ' INNER JOIN `files` AS f ON f.`id` = u.`file_id`'
        insert_history += ' INNER JOIN `{history}`.`files` AS hf ON hf.`name` = f.`name`'
        insert_history += site_joins
        insert_history += ' WHERE q.`id` = %s'

        insert_history = insert_history.format(history = self.history_db, table = table_name, op = optype)

        if optype == 'transfer':
            insert_failure = 'INSERT INTO `failed_transfers` (`id`, `subscription_id`, `source_id`, `exitcode`)'
            insert_failure += ' SELECT `id`, `subscription_id`, `source_id`, %s FROM `transfer_tasks` WHERE `id` = %s'
            insert_failure += ' ON DUPLICATE KEY UPDATE `id`=VALUES(`id`)'
            delete_failures = 'DELETE FROM `failed_transfers` WHERE `subscription_id` = %s'

        get_subscription = 'SELECT u.`id`, u.`status` FROM `{op}_tasks` AS q'.format(op = optype)
        get_subscription += ' INNER JOIN `file_subscriptions` AS u ON u.`id` = q.`subscription_id`'
        get_subscription += ' WHERE q.`id` = %s'

        update_subscription = 'UPDATE `file_subscriptions` SET `status` = %s, `last_update` = NOW() WHERE `id` = %s'
        delete_subscription = 'DELETE FROM `file_subscriptions` WHERE `id` = %s'

        delete_task = 'DELETE FROM `{op}_tasks` WHERE `id` = %s'.format(op = optype)

        delete_batch = 'DELETE FROM `{op}_batches` WHERE `id` = %s'.format(op = optype)

        done_subscriptions = []
        num_success = 0
        num_failure = 0
        num_cancelled = 0

        if optype == 'transfer':
            get_results = self.transfer_query.get_transfer_status
            acknowledge_result = self.transfer_query.forget_transfer_status
            close_batch = self.transfer_query.forget_transfer_batch
        else:
            get_results = self.deletion_query.get_deletion_status
            acknowledge_result = self.deletion_query.forget_deletion_status
            close_batch = self.deletion_query.forget_deletion_batch

        # Collect completed tasks

        for batch_id in self.db.query('SELECT `id` FROM `{op}_batches`'.format(op = optype)):
            results = get_results(batch_id)

            batch_complete = True

            for task_id, status, exitcode, start_time, finish_time in results:
                # start_time and finish_time can be None
                LOG.debug('%s results: %d %s %d %s %s', optype, task_id, status, exitcode, start_time, finish_time)

                if status == FileQuery.STAT_DONE:
                    num_success += 1
                elif status == FileQuery.STAT_FAILED:
                    num_failure += 1
                elif status == FileQuery.STAT_CANCELLED:
                    num_cancelled += 1
                else:
                    batch_complete = False
                    continue

                if not self._read_only:
                    self.db.query(insert_file, task_id)
                    self.db.query(insert_history, exitcode, start_time, finish_time, task_id)

                # We check the subscription status and update accordingly. Need to lock the tables.
                if not self._read_only:
                    self.db.lock_tables(write = ['file_subscriptions', ('file_subscriptions', 'u'), optype + '_tasks', (optype + '_tasks', 'q')])

                try:
                    subscription = self.db.query(get_subscription, task_id)
                    if len(subscription) == 0:
                        # A task without subscription - some sort of state corruption. Just ignore
                        if not self._read_only:
                            self.db.query(delete_task, task_id)
                        continue

                    subscription_id, subscription_status = subscription[0]
    
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

                if optype == 'transfer':
                    if subscription_status == 'cancelled' or (subscription_status == 'inbatch' and status == FileQuery.STAT_DONE):
                        # Delete entries from failed_transfers table
                        self.db.query(delete_failures, subscription_id)

                    elif subscription_status == 'inbatch' and status == FileQuery.STAT_FAILED:
                        # Insert entry to failed_transfers table
                        self.db.query(insert_failure, exitcode, task_id)
    
                if not self._read_only:
                    self.db.query(delete_task, task_id)

                if status == FileQuery.STAT_DONE:
                    done_subscriptions.append(subscription_id)

                acknowledge_result(task_id)

                if self.cycle_stop.is_set():
                    break

            if batch_complete:
                self.db.query(delete_batch, batch_id)
                close_batch(batch_id)

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

        tasks = []

        for subscription in subscriptions:
            if len(subscription.disk_sources) == 0:
                # intelligently random
                source = random.choice(subscription.tape_sources)

            elif len(subscription.disk_sources) == 1:
                source = subscription.disk_sources[0]

            else:
                not_tried = set(subscription.disk_sources)
                if subscription.failed_sources is not None:
                    not_tried -= set(subscription.failed_sources.iterkeys())

                if len(not_tried) != 0 or subscription.failed_sources is None:
                    # intelligently random again
                    source = random.choice(list(not_tried))
                else:
                    # select the least failed site
                    by_failure = sorted(subscription.disk_sources, key = lambda s: subscription.failed_sources[s])
                    source = by_failure[0]
            
            tasks.append(RLFSM.TransferTask(subscription, source))

        return tasks

    def _start_transfers(self, tasks):
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

        result = self.transfer_operation.start_transfers(batch_id, tasks)

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

    def _start_deletions(self, tasks):
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
        
        result = self.deletion_operation.start_deletions(batch_id, tasks)

        successful = [task for task, success in result.iteritems() if success]

        if not self._read_only:
            self.db.execute_many('UPDATE `file_subscriptions` SET `status` = \'inbatch\', `last_update` = NOW()', 'id', [t.desubscription.id for t in successful])

            if len(successful) != len(result):
                failed = [task for task, success in result.iteritems() if not success]

                for task in failed:
                    LOG.error('Cannot delete %s at %s',
                              task.desubscription.file.lfn, task.desubscription.site.name)

                self.db.delete_many('deletion_tasks', 'id', [t.id for t in failed])

                self.db.execute_many('UPDATE `file_subscriptions` SET `status` = \'held\', `last_update` = NOW()', 'id', [t.desubcription.id for t in failed])

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
