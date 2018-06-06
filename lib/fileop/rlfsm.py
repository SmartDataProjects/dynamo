import os
import collections
import random
import time
import logging

from dynamo.fileop.base import FileQuery
from dynamo.fileop.transfer import FileTransferOperation, FileTransferQuery
from dynamo.fileop.deletion import FileDeletionOperation, FileDeletionQuery, DirDeletionOperation
from dynamo.dataformat import Configuration, Block, Site
from dynamo.utils.interface.mysql import MySQL

LOG = logging.getLogger(__name__)

class RLFSM(object):
    """
    File operation manager using MySQL tables for queue bookkeeping. Also implies the
    inventory backend is MySQL.
    """

    class Subscription(object):
        __slots__ = ['id', 'file', 'destination', 'disk_sources', 'tape_sources', 'failed_sources']

        def __init__(self, id, file, destination, disk_sources, tape_sources, failed_sources = None):
            self.id = id
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
        __slots__ = ['id', 'file', 'site']

        def __init__(self, id, file, site):
            self.id = id
            self.file = file
            self.site = site

    class DeletionTask(object):
        __slots__ = ['id', 'desubscription']

        def __init__(self, desubscription):
            self.id = None
            self.desubscription = desubscription


    def __init__(self, config):
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

        self.dry_run = config.get('dry_run', False)
        if self.transfer_operation:
            self.transfer_operation.dry_run = self.dry_run
        if self.deletion_operation:
            self.deletion_operation.dry_run = self.dry_run
        
    def transfer_files(self, inventory):
        def start_transfers(tasks):
            # start the transfer of tasks. If batch submission fails, make progressively smaller batches until failing tasks are identified.
            if self.dry_run:
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

            if not self.dry_run:
                self.db.insert_many('transfer_queue', fields, mapping, tasks)
            
            # set the task ids
            tasks_by_sub = dict((t.subscription.id, t) for t in tasks)
            for task_id, subscription_id in self.db.xquery('SELECT `id`, `subscription_id` FROM `transfer_queue` WHERE `batch_id` = %s', batch_id):
                tasks_by_sub[subscription_id].id = task_id

            self.transfer_operation.dry_run = self.dry_run
            
            success = self.transfer_operation.start_transfers(batch_id, tasks)

            if success:
                if not self.dry_run:
                    self.db.execute_many('UPDATE `file_subscriptions` SET `status` = \'inbatch\', `last_update` = NOW()', 'id', [t.subscription.id for t in tasks])

                return 1, len(tasks)
            else:
                if len(tasks) == 1:
                    task = tasks[0]
                    LOG.error('Cannot start transfer of %s from %s to %s',
                        task.subscription.file.lfn, task.source.name, task.subscription.destination.name)

                    if not self.dry_run:
                        sql = 'INSERT INTO `failed_transfers` (`id`, `subscription_id`, `source_id`, `exitcode`)'
                        sql += ' SELECT `id`, `subscription_id`, `source_id`, %s FROM `transfer_queue` WHERE `id` = %s'
                        self.db.query(sql, -1, task.id)
    
                        sql = 'UPDATE `file_subscriptions` SET `status` = %s, `last_update` = NOW() WHERE `id` = %s'
                        self.db.query(sql, 'retry', task.subscription.id)

                else:
                    LOG.error('Batch transfer of %d files failed. Retrying with smaller batches.', len(tasks))

                if not self.dry_run:
                    # roll back
                    self.db.query('DELETE FROM `transfer_queue` WHERE `batch_id` = %s', batch_id)
                    self.db.query('DELETE FROM `transfer_batches` WHERE `id` = %s', batch_id)

                num_batches, num_tasks = 0, 0

                if len(tasks) > 1:
                    nb, nt = start_transfers(tasks[:len(tasks) / 2])
                    num_batches += nb
                    num_tasks += nt
                    nb, nt = start_transfers(tasks[len(tasks) / 2:])
                    num_batches += nb
                    num_tasks += nt

                return num_batches, num_tasks


        LOG.info('Fetching subscription status from the file operation agent.')
        self._update_status('transfer')

        LOG.info('Collecting new transfer subscriptions.')
        subscriptions = self._get_subscriptions(inventory)

        LOG.info('Identifying source sites for %d transfers.', len(subscriptions))
        tasks = self._select_source(subscriptions)

        LOG.info('Organizing %d transfers into batches.', len(tasks))
        batches = self.transfer_operation.form_batches(tasks)

        LOG.info('Issuing transfer tasks.')
        num_batches, num_tasks = 0, 0
        for batch_tasks in batches:
            nb, nt = start_transfers(batch_tasks)
            num_batches += nb
            num_tasks += nt

        LOG.info('Issued %d transfer tasks in %d batches.', num_tasks, num_batches)

    def delete_files(self, inventory):
        def start_deletions(tasks):
            if self.dry_run:
                batch_id = 0
            else:
                self.db.query('INSERT INTO `deletion_batches` (`id`) VALUES (0)')
                batch_id = self.db.last_insert_id

            # local time
            now = time.strftime('%Y-%m-%d %H:%M:%S')
            
            fields = ('subscription_id', 'batch_id', 'created')
            mapping = lambda t: (t.desubscription.id, batch_id, now)

            if not self.dry_run:
                self.db.insert_many('deletion_queue', fields, mapping, tasks)

            # set the task ids
            tasks_by_sub = dict((t.desubscription.id, t) for t in tasks)
            for task_id, subscription_id in self.db.xquery('SELECT `id`, `subscription_id` FROM `deletion_queue` WHERE `batch_id` = %s', batch_id):
                tasks_by_sub[subscription_id].id = task_id
            
            success = self.deletion_operation.start_deletions(batch_id, tasks)

            if success:
                if not self.dry_run:
                    self.db.execute_many('UPDATE `file_subscriptions` SET `status` = \'inbatch\', `last_update` = NOW()', 'id', [t.desubscription.id for t in tasks])

                return 1, len(tasks)

            else:
                if len(tasks) == 1:
                    task = tasks[0]
                    LOG.error('Cannot delete %s at %s',
                        task.desubscription.file.lfn, task.desubscription.site.name)

                    if not self.dry_run:
                        sql = 'UPDATE `file_subscriptions` SET `status` = %s, `last_update` = NOW() WHERE `id` = %s'
                        self.db.query(sql, 'held', task.desubscription.id)

                else:
                    LOG.error('Batch deletion of %d files failed. Retrying with smaller batches.', len(tasks))

                if not self.dry_run:
                    # roll back
                    self.db.query('DELETE FROM `deletion_queue` WHERE `batch_id` = %s', batch_id)
                    self.db.query('DELETE FROM `deletion_batches` WHERE `id` = %s', batch_id)

                num_batches, num_tasks = 0, 0
                if len(tasks) > 1:
                    nb, nt = start_deletions(tasks[:len(tasks) / 2])
                    num_batches += nb
                    num_tasks += nt
                    nb, nt = start_deletions(tasks[len(tasks) / 2:])
                    num_batches += nb
                    num_tasks += nt

                return num_batches, num_tasks


        LOG.info('Fetching deletion status from the file operation agent.')
        completed = self._update_status('deletion')

        LOG.info('Recording candidates for empty directories.')
        self._set_dirclean_candidates(completed, inventory)

        LOG.info('Collecting new deletion subscriptions.')
        desubscriptions = self._get_desubscriptions(inventory)

        tasks = [RLFSM.DeletionTask(d) for d in desubscriptions]

        LOG.info('Organizing the deletions into batches.')
        batches = self.deletion_operation.form_batches(tasks)

        LOG.info('Issuing deletion tasks.')
        num_batches, num_tasks = 0, 0
        for batch_tasks in batches:
            nb, nt = start_deletions(batch_tasks)
            num_batches += nb
            num_tasks += nt

        LOG.info('Issued %d deletion tasks in %d batches.', num_tasks, num_batches)

    def update_inventory(self, inventory):
        def update_replica(replica, file_ids, projected):
            if replica is None:
                return

            if len(file_ids) == 0 and len(projected) == 0:
                inventory.delete(replica)
            else:
                full_file_ids = set(f.id for f in replica.block.files)

                if file_ids == full_file_ids:
                    replica.file_ids = None
                    inventory.register_update(replica)
                else:
                    if replica.file_ids is None:
                        existing_file_ids = full_file_ids
                    else:
                        existing_file_ids = set(replica.file_ids)

                    if file_ids != existing_file_ids:
                        replica.file_ids = tuple(file_ids)
                        inventory.register_update(replica)


        ## List all subscriptions in block, site, time order
        sql = 'SELECT u.`id`, u.`status`, u.`delete`, d.`name`, b.`name`, u.`file_id`, s.`name` FROM `file_subscriptions` AS u'
        sql += ' INNER JOIN `files` AS f ON f.`id` = u.`file_id`'
        sql += ' INNER JOIN `blocks` AS b ON b.`id` = f.`block_id`'
        sql += ' INNER JOIN `datasets` AS d ON d.`id` = b.`dataset_id`'
        sql += ' INNER JOIN `sites` AS s ON s.`id` = u.`site_id`'
        sql += ' ORDER BY d.`id`, b.`id`, s.`id`, u.`last_update`'
        
        _dataset_name = ''
        _block_name = ''
        _site_name = ''
        
        dataset = None
        block = None
        site = None
        replica = None
        file_ids = None
        projected = set()
        
        COPY = 0
        DELETE = 1
        
        for sub_id, status, optype, dataset_name, block_name, file_id, site_name in self.db.xquery(sql):
            if dataset_name != _dataset_name:
                _dataset_name = dataset_name
                # dataset must exist
                dataset = inventory.datasets[dataset_name]
        
                _block_name = ''
        
            if block_name != _block_name:
                _block_name = block_name
                # block must exist
                block = dataset.find_block(Block.to_internal_name(block_name), must_find = True)
        
            if site_name != _site_name:
                _site_name = site_name
                # site must exist
                site = inventory.sites[site_name]
        
            if replica is None or replica.block != block or replica.site != site:
                # check updates for previous replica
                update_replica(replica, file_ids, projected)

                replica = block.find_replica(site)
                if replica.file_ids is None:
                    file_ids = set(f.id for f in block.files)
                else:
                    file_ids = set(replica.file_ids)

                projected.clear()
        
            if optype == COPY:
                if status == 'done':
                    file_ids.add(file_id)
                else:
                    projected.add(file_id)
            elif optype == DELETE:
                try:
                    projected.remove(file_id)
                except KeyError:
                    pass

                if status == 'done':
                    try:
                        file_ids.remove(file_id)
                    except KeyError:
                        pass

        update_replica(replica, file_ids, projected)

        if not self.dry_run:
            # this is dangerous - what if inventory fails to update on the server side?
            self.db.query('DELETE FROM `file_subscriptions` WHERE `status` = \'done\'')

    def subscribe_files(self, block_replica):
        """
        Make subscriptions of missing files in the block replica.
        """
        if block_replica.file_ids is None:
            # replica supposedly has all files
            LOG.info('No files to subscribe for %s', str(block_replica))
            return

        all_ids = set(f.id for f in block_replica.block.files)
        missing_ids = all_ids - set(block_replica.file_ids)

        LOG.info('Subscribing %d files from %s', len(missing_ids), str(block_replica))

        site_id = block_replica.site.id

        # local time
        now = time.strftime('%Y-%m-%d %H:%M:%S')

        fields = ('file_id', 'site_id', 'delete', 'created')
        mapping = lambda f: (f, site_id, 0, now)

        if not self.dry_run:
            self.db.insert_many('file_subscriptions', fields, mapping, missing_ids)

    def desubscribe_files(self, site, files):
        """
        Book deletion of files at the site.
        """
        LOG.info('Desubscribing %d files from %s', len(files), site.name)

        # local time
        now = time.strftime('%Y-%m-%d %H:%M:%S')

        fields = ('file_id', 'site_id', 'delete', 'created')
        mapping = lambda f: (f.id, site.id, 1, now)

        if not self.dry_run:
            self.db.insert_many('file_subscriptions', fields, mapping, files)

    def _update_status(self, optype):
        insert_file = 'INSERT INTO `{history}`.`files` (`name`, `size`)'
        insert_file += ' SELECT f.`name`, f.`size` FROM `transfer_queue` AS q'
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
            delete_val = '0'
        else:
            table_name = 'file_deletions'
            site_fields = '`site_id`'
            site_values = 'hs.`id`'
            site_joins = ' INNER JOIN `sites` AS s ON s.`id` = u.`site_id`'
            site_joins += ' INNER JOIN `{history}`.`sites` AS hs ON hs.`name` = s.`name`'
            delete_val = '1'

        insert_history = 'INSERT INTO `{history}`.`{table}`'
        insert_history += ' (`id`, `file_id`, ' + site_fields + ', `exitcode`, `batch_id`, `created`, `started`, `finished`, `completed`)'
        insert_history += ' SELECT q.`id`, hf.`id`, ' + site_values + ', %s, q.`batch_id`, q.`created`, FROM_UNIXTIME(%s), FROM_UNIXTIME(%s), NOW()'
        insert_history += ' FROM `transfer_queue` AS q'
        insert_history += ' INNER JOIN `file_subscriptions` AS u ON u.`id` = q.`subscription_id`'
        insert_history += ' INNER JOIN `files` AS f ON f.`id` = u.`file_id`'
        insert_history += ' INNER JOIN `{history}`.`files` AS hf ON hf.`name` = f.`name`'
        insert_history += site_joins
        insert_history += ' WHERE u.`delete` = ' + delete_val + ' AND q.`id` = %s'

        insert_history = insert_history.format(history = self.history_db, table = table_name)

        if optype == 'transfer':
            insert_failure = 'INSERT INTO `failed_transfers` (`id`, `subscription_id`, `source_id`, `exitcode`)'
            insert_failure += ' SELECT `id`, `subscription_id`, `source_id`, %s FROM `transfer_queue` WHERE `id` = %s'
            delete_failures = 'DELETE FROM `failed_transfers` WHERE `subscription_id` = %s'

        get_subscription = 'SELECT `subscription_id` FROM `{op}_queue` WHERE `id` = %s'.format(op = optype)

        update_subscription = 'UPDATE `file_subscriptions` SET `status` = %s, `last_update` = NOW() WHERE `id` = %s'

        delete_queue = 'DELETE FROM `{op}_queue` WHERE `id` = %s'.format(op = optype)

        completed_subscriptions = []
        num_success = 0
        num_failure = 0

        if optype == 'transfer':
            get_results = lambda batch_id: self.transfer_query.get_transfer_status(batch_id)
        else:
            get_results = lambda batch_id: self.deletion_query.get_deletion_status(batch_id)

        for batch_id in self.db.query('SELECT `id` FROM `{op}_batches`'.format(op = optype)):
            results = get_results(batch_id)

            for task_id, status, exitcode, start_time, finish_time in results:
                if status == FileQuery.STAT_DONE:
                    num_success += 1
                elif status == FileQuery.STAT_FAILED:
                    num_failure += 1
                else:
                    continue

                if not self.dry_run:
                    self.db.query(insert_file, task_id)
                    self.db.query(insert_history, exitcode, start_time, finish_time, task_id)

                subscription_id = self.db.query(get_subscription, task_id)[0]

                if not self.dry_run:
                    if status == FileQuery.STAT_DONE:
                        LOG.debug('Subscription %d completed.', optype, subscription_id)
                        self.db.query(update_subscription, 'done', subscription_id)
                        if optype == 'transfer':
                            # Delete entries from failed_transfers table
                            self.db.query(delete_failures, subscription_id)
                    else:
                        LOG.debug('Subscription %d failed (exit code %d). Flagging retry.', subscription_id, exitcode)
                        self.db.query(update_subscription, 'retry', subscription_id)
                        if optype == 'transfer':
                            # Insert entry to failed_transfers table
                            self.db.query(insert_failure, exitcode, task_id)
    
                    self.db.query(delete_queue, task_id)

                if status == FileQuery.STAT_DONE:
                    completed_subscriptions.append(subscription_id)

        LOG.info('Archived file %s: %d succeeded, %d failed.', optype, num_success, num_failure)

        return completed_subscriptions

    def _get_subscriptions(self, inventory):
        subscriptions = []

        get_all = 'SELECT u.`id`, u.`status`, d.`name`, b.`name`, f.`id`, f.`name`, s.`name` FROM `file_subscriptions` AS u'
        get_all += ' INNER JOIN `files` AS f ON f.`id` = u.`file_id`'
        get_all += ' INNER JOIN `blocks` AS b ON b.`id` = f.`block_id`'
        get_all += ' INNER JOIN `datasets` AS d ON d.`id` = b.`dataset_id`'
        get_all += ' INNER JOIN `sites` AS s ON s.`id` = u.`site_id`'
        get_all += ' WHERE u.`delete` = 0 AND u.`status` IN (\'new\', \'retry\')'
        get_all += ' ORDER BY d.`id`, b.`id`, s.`id`'

        get_tried_sites = 'SELECT s.`name`, f.`exitcode` FROM `failed_transfers` AS f'
        get_tried_sites += ' INNER JOIN `sites` AS s ON s.`id` = f.`source_id`'
        get_tried_sites += ' WHERE f.`subscription_id` = %s'

        _dataset_name = ''
        _block_name = ''
        _site_name = ''

        dataset = None
        block = None
        destination = None

        to_hold = []

        for row in self.db.query(get_all):
            sub_id, status, dataset_name, block_name, file_id, file_name, site_name = row

            if dataset_name != _dataset_name:
                _dataset_name = dataset_name
                # dataset must exist
                dataset = inventory.datasets[dataset_name]

                _block_name = ''

            if block_name != _block_name:
                _block_name = block_name
                # block must exist
                block = dataset.find_block(Block.to_internal_name(block_name), must_find = True)

            if site_name != _site_name:
                _site_name = site_name
                # site must exist
                destination = inventory.sites[site_name]

            lfile = block.find_file(file_name, must_find = True)

            disk_sources = []
            tape_sources = []
            for replica in block.replicas:
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
                continue

            subscription = RLFSM.Subscription(sub_id, lfile, destination, disk_sources, tape_sources)

            if status == 'retry':
                subscription.failed_sources = {}
                for source_name, exitcode in self.db.query(get_tried_sites):
                    source = inventory.sites[source_name]
                    if source not in subscription.failed_sources:
                        subscription.failed_sources[source] = [exitcode]
                    else:
                        subscription.failed_sources[source].append(exitcode)
    
            subscriptions.append(subscription)

        if not self.dry_run:
            self.db.execute_many('UPDATE `file_subscriptions` SET `status` = \'held\', `last_update` = NOW()', 'id', to_hold)

        return subscriptions

    def _get_desubscriptions(self, inventory):
        desubscriptions = []

        get_all = 'SELECT u.`id`, u.`status`, d.`name`, b.`name`, f.`id`, f.`name`, s.`name` FROM `file_subscriptions` AS u'
        get_all += ' INNER JOIN `files` AS f ON f.`id` = u.`file_id`'
        get_all += ' INNER JOIN `blocks` AS b ON b.`id` = f.`block_id`'
        get_all += ' INNER JOIN `datasets` AS d ON d.`id` = b.`dataset_id`'
        get_all += ' INNER JOIN `sites` AS s ON s.`id` = u.`site_id`'
        get_all += ' WHERE u.`delete` = 1 AND u.`status` IN (\'new\', \'retry\')'
        get_all += ' ORDER BY d.`id`, b.`id`, s.`id`'

        _dataset_name = ''
        _block_name = ''
        _site_name = ''

        dataset = None
        block = None
        site = None

        for row in self.db.query(get_all):
            desub_id, status, dataset_name, block_name, file_id, file_name, site_name = row

            if dataset_name != _dataset_name:
                _dataset_name = dataset_name
                # dataset must exist
                dataset = inventory.datasets[dataset_name]

                _block_name = ''

            if block_name != _block_name:
                _block_name = block_name
                # block must exist
                block = dataset.find_block(Block.to_internal_name(block_name), must_find = True)

            if site_name != _site_name:
                _site_name = site_name
                # site must exist
                site = inventory.sites[site_name]

            lfile = block.find_file(file_name, must_find = True)

            desubscription = RLFSM.Desubscription(desub_id, lfile, site)
    
            desubscriptions.append(desubscription)

        return desubscriptions

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
                not_tried = set(subscription.disk_sources) - set(subscription.failed_sources.iterkeys())
                if len(not_tried) != 0:
                    # intelligently random again
                    source = random.choice(not_tried)
                else:
                    # select the least failed site
                    by_failure = sorted(subscription.disk_sources, key = lambda s: subscription.failed_sources[s])
                    source = by_failure[0]
            
            tasks.append(RLFSM.TransferTask(subscription, source))

        return tasks
    
    def _set_dirclean_candidates(self, subscription_ids, inventory):
        site_dirs = {}

        # Clean up directories of completed subscriptions
        sql = 'SELECT s.`name`, f.`name` FROM `file_subscriptions` AS u'
        sql += ' INNER JOIN `files` AS f ON f.`id` = u.`file_id`'
        sql += ' INNER JOIN `sites` AS s ON s.`id` = u.`site_id`'

        for site_name, file_name in self.db.execute_many(sql, 'u.`id`', subscription_ids):
            site = inventory.sites[site_name]

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
        if not self.dry_run:
            self.db.insert_many('directory_cleaning_queue', fields, None, get_entry(), do_update = True)
