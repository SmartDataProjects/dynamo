import os
import collections
import random
import time
import logging

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
        # Transfer protocol to use (necessary for LFN-to-PFN mapping)
        self.protocol = config.protocol

        # Handle to the inventory DB
        self.db = MySQL(config.db.db_params)

        # History DB name
        self.history_db = config.db.history

        # FileTransferOperation backend (can make it a map from (source, dest) to operator)
        self.transfer_operation = FileTransferOperation.get_instance(config.transfer.module, config.transfer.config)

        # QueryOperation backend
        if 'transfer_query' in config:
            self.transfer_query = FileTransferQuery.get_instance(config.transfer_query.module, config.transfer_query.config)
        else:
            self.transfer_query = self.transfer_operation

        # FileDeletionOperation backend (can make it a map from dest to operator)
        self.deletion_operation = FileDeletionOperation.get_instance(config.deletion.module, config.deletion.config)

        # QueryOperation backend
        if 'deletion_query' in config:
            self.deletion_query = FileDeletionQuery.get_instance(config.deletion_query.module, config.deletion_query.config)
        else:
            self.deletion_query = self.deletion_operation

        self.dry_run = config.get('dry_run', False)
        self.transfer_operation.dry_run = self.dry_run
        self.deletion_operation.dry_run = self.dry_run
        
    def transfer_files(self, inventory):
        def start_transfers(tasks):
            # start the transfer of tasks. If batch submission fails, make progressively smaller batches until failing tasks are identified.
            if self.dry_run:
                batch_id = 0
            else:
                self.db.query('INSERT INTO `transfer_batches`')
                batch_id = self.db.last_insert_id

            # local time
            now = time.strftime('%Y-%m-%d %H:%M:%S')

            # need to create the transfer tasks first to have ids assigned
            fields = ('subscription_id', 'source', 'batch_id', 'created')
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
            else:
                if len(tasks) == 1:
                    task = tasks[0]
                    LOG.error('Cannot start transfer of %s from %s to %s',
                        task.subscription.file.lfn, task.source.name, task.subscription.destination.name)

                    if not self.dry_run:
                        sql = 'INSERT INTO `failed_transfers` (`id`, `subscription_id`, `source`, `exitcode`)'
                        sql += ' SELECT `id`, `subscription_id`, `source`, %s FROM `transfer_queue` WHERE `id` = %s'
                        self.db.query(sql, -1, task.id)
    
                        sql = 'UPDATE `file_subscriptions` SET `status` = %s, `last_update` = NOW() WHERE `id` = %s'
                        self.db.query(sql, 'retry', task.subscription.id)

                else:
                    LOG.error('Batch transfer of %d files failed. Retrying with smaller batches.', len(tasks))

                if not self.dry_run:
                    # roll back
                    self.db.query('DELETE FROM `transfer_queue` WHERE `batch_id` = %s', batch_id)
                    self.db.query('DELETE FROM `transfer_batches` WHERE `id` = %s', batch_id)

                if len(tasks) > 1:
                    start_transfers(tasks[:len(tasks) / 2])
                    start_transfers(tasks[len(tasks) / 2:])


        self._update_subscription_status()

        subscriptions = self._get_subscriptions(inventory)

        tasks = self._select_source(subscriptions)

        batches = self.transfer_operation.form_batches(tasks)

        for batch_tasks in batches:
            start_transfers(batch_tasks)

    def delete_files(self, inventory):
        def start_deletions(tasks):
            if self.dry_run:
                batch_id = 0
            else:
                self.db.query('INSERT INTO `deletion_batches`')
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

                if len(tasks) > 1:
                    start_deletions(tasks[:len(tasks) / 2])
                    start_deletions(tasks[len(tasks) / 2:])


        completed = self._update_deletion_status()

        self._set_dirclean_candidates(completed, inventory)
          
        desubscriptions = self._get_desubscriptions(inventory)

        tasks = [RLFSM.DeletionTask(d) for d in desubscriptions]

        batches = self.deletion_operation.form_batches(tasks)

        for batch_tasks in batches:
            start_deletions(batch_tasks)

    def update_inventory(self, inventory):
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
                if replica is not None and file_ids != set(replica.file_ids):
                    if len(file_ids) == 0 and len(projected) == 0:
                        inventory.delete(replica)
                    elif file_ids != set(replica.file_ids):
                        replica.file_ids = tuple(file_ids)
                        inventory.register_update(replica)

                replica = block.find_replica(site)
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
        
        if replica is not None:
            if len(file_ids) == 0 and len(projected) == 0:
                inventory.delete(replica)
            elif file_ids != set(replica.file_ids):
                replica.file_ids = tuple(file_ids)
                inventory.register_update(replica)

        if not self.dry_run:
            # this is dangerous - what if inventory fails to update on the server side?
            self.db.query('DELETE FROM `file_subscriptions` WHERE `status` = \'done\'')

    def _subscribe_files(self, block_replica):
        """
        Make subscriptions of missing files in the block replica.
        """
        all_ids = set(f.id for f in block_replica.block.files)
        missing_ids = all_ids - set(block_replica.file_ids)

        site_id = block_replica.site.id

        # local time
        now = time.strftime('%Y-%m-%d %H:%M:%S')

        fields = ('file_id', 'site_id', 'delete', 'created')
        mapping = lambda f: (f, site_id, 0, now)

        if not self.dry_run:
            self.db.insert_many('file_subscriptions', fields, mapping, missing_ids)

    def _desubscribe_files(self, block_replica):
        """
        Book deletion of files in the block replica.
        """
        site_id = block_replica.site.id

        # local time
        now = time.strftime('%Y-%m-%d %H:%M:%S')

        fields = ('file_id', 'site_id', 'delete', 'created')
        mapping = lambda f: (f, site_id, 1, now)

        if not self.dry_run:
            self.db.insert_many('file_subscriptions', fields, mapping, block_replica.file_ids)

    def _update_subscription_status(self):
        insert_file = 'INSERT INTO `{history}`.`files` (`name`)'
        insert_file += ' SELECT f.`name` FROM `transfer_queue` AS q'
        insert_file += ' INNER JOIN `file_subscriptions` AS u ON u.`id` = q.`subscription_id`'
        insert_file += ' INNER JOIN .`files` AS f ON f.`id` = u.`file_id`'
        insert_file += ' WHERE u.`delete` = 0 AND q.`id` = %s'
        insert_file += ' ON DUPLICATE KEY UPDATE `files`=VALUES(`files`)'

        insert_file = insert_file.format(history = self.history_db)

        # sites have to be inserted to history already

        insert_transfer = 'INSERT INTO `{history}`.`file_transfers` (`id`, `file_id`, `source_id`, `destination_id`, `exitcode`, `batch_id`, `created`, `completed`)'
        insert_transfer += ' SELECT q.`id`, hf.`id`, hss.`id`, hsd.`id`, %s, q.`batch_id`, q.`created`, FROM_UNIXTIME(%s) FROM `transfer_queue`'
        insert_transfer += ' INNER JOIN `file_subscriptions` AS u ON u.`id` = q.`subscription_id`'
        insert_transfer += ' INNER JOIN `files` AS f ON f.`id` = u.`file_id`'
        insert_transfer += ' INNER JOIN `sites` AS sd ON sd.`id` = u.`site_id`'
        insert_transfer += ' INNER JOIN `sites` AS ss ON ss.`id` = q.`source`'
        insert_transfer += ' INNER JOIN `{history}`.`files` AS hf ON hf.`name` = f.`name`'
        insert_transfer += ' INNER JOIN `{history}`.`sites` AS hsd ON hsd.`name` = sd.`name`'
        insert_transfer += ' INNER JOIN `{history}`.`sites` AS hss ON hss.`name` = ss.`name`'
        insert_transfer += ' WHERE u.`delete` = 0 AND q.`id` = %s'

        insert_transfer = insert_transfer.format(history = self.history_db)

        insert_failure = 'INSERT INTO `failed_transfers` (`id`, `subscription_id`, `source`, `exitcode`)'
        insert_failure += ' SELECT `id`, `subscription_id`, `source`, %s FROM `transfer_queue` WHERE `id` = %s'

        get_subscription = 'SELECT `subscription_id` FROM `transfer_queue` WHERE `id` = %s'

        update_subscription = 'UPDATE `file_subscriptions` SET `status` = %s, `last_update` = NOW() WHERE `id` = %s'

        delete_failures = 'DELETE FROM `failed_transfers` WHERE `subscription_id` = %s'

        delete_transfer = 'DELETE FROM `transfer_queue` WHERE `id` = %s'

        sql = 'SELECT `id` FROM `transfer_batches`'
        for batch_id in self.db.query(sql):
            transfer_results = self.transfer_query.get_status(batch_id)

            for transfer_id, status, exitcode, finish_time in transfer_results:
                if status not in (FileTransferQuery.STAT_DONE, FileTransferQuery.STAT_FAILED):
                    continue

                if not self.dry_run:
                    self.db.query(insert_file, transfer_id)
                    self.db.query(insert_transfer, exitcode, finish_time, transfer_id)

                subscription_id = self.db.query(get_subscription, transfer_id)[0]

                if not self.dry_run:
                    if status == FileTransferQuery.STAT_DONE:
                        self.db.query(update_subscription, 'done', subscription_id)
                        self.db.query(delete_failures, subscription_id)
                    else:
                        self.db.query(insert_failure, exitcode, transfer_id)
                        self.db.query(update_subscription, 'retry', subscription_id)
    
                    self.db.query(delete_transfer, transfer_id)

    def _update_deletion_status(self):
        insert_file = 'INSERT INTO `{history}`.`files` (`name`)'
        insert_file += ' SELECT f.`name` FROM `deletion_queue` AS q'
        insert_file += ' INNER JOIN `file_subscriptions` AS u ON u.`id` = q.`subscription_id`'
        insert_file += ' INNER JOIN `files` AS f ON f.`id` = u.`file_id`'
        insert_file += ' WHERE u.`delete` = 1 AND q.`id` = %s'
        insert_file += ' ON DUPLICATE KEY UPDATE `files`=VALUES(`files`)'

        insert_file = insert_file.format(history = self.history_db)

        # sites have to be inserted to history already

        insert_deletion = 'INSERT INTO `{history}`.`file_deletions` (`id`, `file_id`, `site_id`, `exitcode`, `batch_id`, `created`, `completed`)'
        insert_deletion += ' SELECT q.`id`, hf.`id`, hs.`id`, %s, q.`batch_id`, q.`created`, FROM_UNIXTIME(%s) FROM `deletion_queue`'
        insert_deletion += ' INNER JOIN `file_subscriptions` AS u ON u.`id` = q.`subscription_id`'
        insert_deletion += ' INNER JOIN `files` AS f ON f.`id` = u.`file_id`'
        insert_deletion += ' INNER JOIN `sites` AS s ON s.`id` = u.`site_id`'
        insert_deletion += ' INNER JOIN `{history}`.`files` AS hf ON hf.`name` = f.`name`'
        insert_deletion += ' INNER JOIN `{history}`.`sites` AS hs ON hs.`name` = s.`name`'
        insert_deletion += ' WHERE u.`delete` = 1 AND q.`id` = %s'

        insert_deletion = insert_deletion.format(history = self.history_db)

        get_subscription = 'SELECT `subscription_id` FROM `deletion_queue` WHERE `id` = %s'

        update_subscription = 'UPDATE `file_subscriptions` SET `status` = %s, `last_update` = NOw() WHERE `id` = %s'

        delete_deletion = 'DELETE FROM `deletion_queue` WHERE `id` = %s'

        completed_subscriptions = []

        sql = 'SELECT `id` FROM `deletion_batches`'
        for batch_id in self.db.query(sql):
            deletion_results = self.deletion_query.get_status(batch_id)

            for deletion_id, status, exitcode, finish_time in deletion_results:
                if status not in (FileDeletionQuery.STAT_DONE, FileDeletionQuery.STAT_FAILED):
                    continue

                if not self.dry_run:
                    self.db.query(insert_file, deletion_id)
                    self.db.query(insert_deletion, exitcode, finish_time, deletion_id)

                subscription_id = self.db.query(get_subscription, deletion_id)[0]

                if not self.dry_run:
                    if status == FileDeletionQuery.STAT_DONE:
                        self.db.query(update_subscription, 'done', subscription_id)
                    else:
                        self.db.query(update_subscription, 'retry', subscription_id)
    
                    self.db.query(delete_deletion, deletion_id)

                if status == FileDeletionQuery.STAT_DONE:
                    completed_subscriptions.append(subscription_id)

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

        get_tried_sites = 'SELECT s.`name`, f.`exitcode` FROM `failed_transfers`'
        get_tried_sites += ' INNER JOIN `sites` AS s ON s.`id` = f.`source`'
        get_tried_sites += ' WHERE `subscription_id` = %s'

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
