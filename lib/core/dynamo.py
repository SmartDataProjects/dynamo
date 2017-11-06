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

    def __init__(self):
        pass

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

        db_config = dict(common_config.mysql)
        db_config['db'] = 'dynamoregister'
        registry = MySQL(**db_config)

        child_processes = []

        while True:
            try:
                query = 'SELECT s.`id`, s.`title`, s.`path`, s.`user_id`, u.`name`, s.`body`, s.`write_request`'
                query += ' FROM `uploaded_executables` AS s INNER JOIN `users` AS u ON u.`id` = s.`user_id`'
                query += ' LIMIT 1'
                result = registry.query(query)

                if len(result) == 0:
                    time.sleep(5)
                    continue

                else:
                    exec_id, title, path, user_id, user_name, body, write_request = result[0]
                    registry.query('DELETE FROM `uploaded_executables` WHERE `id` = %s', exec_id)

                    if write_request:
                        # check authorization
                        if body is None:
                            checksum = None
                        else:
                            checksum = hashlib.md5(body).hexdigest()

                        query = 'SELECT `user_id` FROM `authorized_executables` WHERE `title` = %s AND `checksum` = %s'
                        for auth_user_id in registry.query(query, title, checksum):
                            if auth_user_id == 0 or auth_user_id == user_id:
                                break
                        else:
                            # send a message
                            continue

                        queue = multiprocessing.Queue(64)

                    else:
                        queue = None

                    proc = multiprocessing.Process(target = Dynamo._run_one, name = title, args = (inventory, body, queue))
                    child_processes.append((proc, user_name, path, queue))

                    proc.daemon = True
                    proc.start()

                    LOG.info('Started executable %s (%s) requested by %s (PID %d).', title, path, user_name, proc.pid)

                ichild = 0
                while ichild != len(child_processes):
                    proc, user_name, path, queue = child_processes[ichild]

                    if queue is not None and not queue.empty():
                        # the child process is wrapping up and wants to send us the list of updated objects
                        # pool all updated objects into a list first
                        updated_objects = []

                        while True:
                            try:
                                obj = queue.get(block = True, timeout = 1)
                            except Queue.Empty:
                                if proc.is_alive():
                                    # still trying to say something
                                    continue
                                else:
                                    break

                            updated_objects.append(obj)

                        if len(updated_objects) != 0:
                            # process data
                            sigint.block()
                            inventory.update_objects(updated_objects)
                            sigint.unblock()

                    if proc.is_alive():
                        ichild += 1
                    else:
                        LOG.info('Executable %s (%s) requested by %s completed (Exit code %d).', proc.name, path, user_name, proc.exitcode)
                        child_processes.pop(ichild)

                time.sleep(1)

            except KeyboardInterrupt:
                LOG.info('Main process interrupted with SIGINT.')
                break

            except:
                # log the exception
                LOG.warning('Exception in main process. Terminating all child processes.')
                break

            finally:
                for proc, user_name, path, pipe in child_processes:
                    LOG.warning('Terminating %s (%s) requested by %s (PID %d)', proc.name, path, user_name, proc.pid)
                    proc.terminate()
                    proc.join(5)
                    if proc.is_alive():
                        LOG.warning('Child process %d did not return after 5 seconds.', proc.pid)

        
    @staticmethod
    def _run_one(inventory, executable, queue):
        if pipe is not None:
            # create a list of updated objects the executable can fill
            inventory.updated_objects = []

        exec(executable, {'dynamo': inventory})

        if queue is not None:
            for obj in inventory.updated_objects:
                queue.put(obj)

            # can we close and quit here?
            while not queue.empty():
                time.sleep(1)
