import sys
import time
import logging
import hashlib
import multiprocessing
import Queue

from core.inventory import DynamoInventory
from common.interface.mysql import MySQL
from common.configuration import common_config
from common.control import sigint

LOG = logging.getLogger(__name__)

class Dynamo(object):
    """Main daemon class."""

    CMD_UPDATE, CMD_DELETE = range(2)

    def __init__(self):
        db_config = dict(common_config.mysql)
        db_config['db'] = 'dynamoregister'
        db_config['reuse_connection'] = False
        self.registry = MySQL(**db_config)

    def run(self):
        """
        Infinite-loop main body of the daemon.
        Step 1: Poll the registry for one uploaded script.
        Step 2: If a script is found, check the authorization of the script.
        Step 3: Spawn a child process for the script with either a partition or the inventory as an argument.
        Step 4: Collect completed child processes.
        Step 5: Sleep for N seconds.
        """

        LOG.info('Started dynamo daemon.')

        inventory = DynamoInventory()

        child_processes = []

        LOG.info('Start polling for executables.')
        sleep_time = 0

        try:
            while True:
                LOG.debug('Polling for executables.')
    
                sql = 'SELECT s.`id`, s.`title`, s.`file`, s.`user_id`, u.`name`, s.`content`, s.`type`, s.`write_request`'
                sql += ' FROM `action` AS s INNER JOIN `users` AS u ON u.`id` = s.`user_id`'
                sql += ' WHERE s.`status` = \'new\''
                sql += ' ORDER BY s.`timestamp` LIMIT 1'
                result = self.registry.query(sql)

                if len(result) == 0:
                    if sleep_time == 0:
                        LOG.info('Waiting for executables.')
                    else:
                        LOG.debug('No executable found, sleeping for 5 seconds.')

                    sleep_time = 5
    
                else:
                    sleep_time = 0

                    exec_id, title, path, user_id, user_name, content, content_type, write_request = result[0]
                    self.registry.query('UPDATE `action` SET `status` = \'run\' WHERE `id` = %s', exec_id)

                    if content_type == 'deletion_policy':
                        # this is a detox test run
                        pass
                    elif content_type == 'executable':
                        LOG.info('Found executable %s from user %s (write request: %s)', title, user_name, write_request)

                        if write_request:
                            if not self.check_write_auth(title, user_id, content):
                                LOG.warning('Executable %s from user %s is not authorized for write access.', title, user_name)
                                # send a message
                                continue
        
                            queue = multiprocessing.Queue(64)
                        else:
                            queue = None
        
                        proc = multiprocessing.Process(target = Dynamo._run_one, name = title, args = (inventory, content, queue))
                        child_processes.append((exec_id, proc, user_name, path, queue))
        
                        proc.daemon = True
                        proc.start()
        
                        LOG.info('Started executable %s (%s) from user %s (PID %d).', title, path, user_name, proc.pid)

                self.collect_processes(inventory, child_processes)
   
                time.sleep(sleep_time)

        except KeyboardInterrupt:
            LOG.info('Main process interrupted with SIGINT.')

        except:
            # log the exception
            LOG.warning('Exception in main process. Terminating all child processes.')

        finally:
            for exec_id, proc, user_name, path, pipe in child_processes:
                LOG.warning('Terminating %s (%s) requested by %s (PID %d)', proc.name, path, user_name, proc.pid)
                proc.terminate()
                proc.join(5)
                if proc.is_alive():
                    LOG.warning('Child process %d did not return after 5 seconds.', proc.pid)

                self.registry.query('UPDATE `action` SET `status` = \'killed\' where `id` = %s', exec_id)

    def check_write_auth(self, title, user, content):
        # check authorization
        if content is None:
            checksum = None
        else:
            checksum = hashlib.md5(content).hexdigest()
        
        sql = 'SELECT `user_id` FROM `authorized_executables` WHERE `title` = %s AND `checksum` = %s'
        for auth_user_id in self.registry.query(sql, title, checksum):
            if auth_user_id == 0 or auth_user_id == user_id:
                return True

        return False

    def collect_processes(self, inventory, child_processes):
        ichild = 0
        while ichild != len(child_processes):
            exec_id, proc, user_name, path, queue = child_processes[ichild]
    
            if queue is not None and not queue.empty():
                # the child process is wrapping up and wants to send us the list of updated objects
                # pool all updated objects into a list first
                updated_objects = []
                deleted_objects = []
    
                while True:
                    try:
                        cmd, obj = queue.get(block = True, timeout = 1)
                    except Queue.Empty:
                        if proc.is_alive():
                            # still trying to say something
                            continue
                        else:
                            break
                    else:
                        if cmd == Dynamo.CMD_UPDATE:
                            updated_objects.append(obj)
                        elif cmd == Dynamo.CMD_DELETE:
                            deleted_objects.append(obj)
    
                if len(updated_objects) != 0 or len(deleted_objects) != 0:
                    # process data
                    sigint.block()
                    for obj in updated_objects:
                        inventory.update(obj)
                    for obj in deleted_objects:
                        inventory.delete(obj)
                    sigint.unblock()
    
            if proc.is_alive():
                ichild += 1
            else:
                LOG.info('Executable %s (%s) from user %s completed (Exit code %d).', proc.name, path, user_name, proc.exitcode)
                child_processes.pop(ichild)

                self.registry.query('UPDATE `action` SET `status` = %s where `id` = %s', 'done' if proc.exitcode == 0 else 'fail', exec_id)

        
    @staticmethod
    def _run_one(inventory, executable, queue):
        if queue is not None:
            # create a list of updated objects the executable can fill
            inventory._updated_objects = []
            inventory._deleted_objects = []

        sys.stdout = open('/tmp/test.out', 'w')

        exec(executable, {'dynamo': inventory})

        if queue is not None:
            for obj in inventory._updated_objects:
                queue.put((Dynamo.CMD_UPDATE, obj))
            for obj in inventory._deleted_objects:
                queue.put((Dynamo.CMD_DELETE, obj))

            # can we close and quit here?
            while not queue.empty():
                time.sleep(1)

        sys.stdout.close()
