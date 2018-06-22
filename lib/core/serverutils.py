import os
import sys
import signal
import multiprocessing
import logging
from ctypes import cdll

libc = cdll.LoadLibrary("/lib64/libc.so.6") # will use glibc mount()

from dynamo.core.inventory import DynamoInventory
from dynamo.utils.log import log_exception, reset_logger
from dynamo.utils.path import find_common_base

BANNER = '''
+++++++++++++++++++++++++++++++++++++
++++++++++++++ DYNAMO +++++++++++++++
++++++++++++++  v2.2  +++++++++++++++
+++++++++++++++++++++++++++++++++++++
'''

def killproc(proc, LOG, timeout = 5):
    try:
        proc.terminate()
    except OSError:
        LOG.error('Exception terminating process %d', proc.pid)
        log_exception(LOG)
    else:
        proc.join(timeout)

    if proc.is_alive():
        # if still alive, force kill
        try:
            os.kill(proc.pid, signal.SIGKILL)
        except:
            pass

        proc.join(1)

def bindmount(source, target):
    # Enums defined in sys/mount.h - not named variables in libc.so
    RDONLY = 1
    REMOUNT = 32
    BIND = 4096

    uid = os.geteuid()
    gid = os.getegid()
    os.seteuid(0)
    os.setegid(0)

    # glibc mount() requires mount-remount to have a read-only bind mount
    libc.mount(source, target, None, BIND, None)
    libc.mount(source, target, None, RDONLY | REMOUNT | BIND, None)

    os.setegid(gid)
    os.seteuid(uid)

def umount(path):
    uid = os.geteuid()
    gid = os.getegid()
    os.seteuid(0)
    os.setegid(0)

    libc.umount(path)

    os.setegid(gid)
    os.seteuid(uid)

def umountall(path):
    # Undo bindmounts done in pre_execution
    for mount in find_common_base(map(os.path.realpath, sys.path)):
        umount(path + mount)

def clean_remote_request(path):
    # Since threads cannot change the uid, we launch a subprocess.
    # (Mounts are made read-only, so there is no risk of accidents even if the subprocess fails)
    proc = multiprocessing.Process(target = umountall, args = (path,))
    proc.start()
    proc.join()

def pre_execution(path, is_local, read_only, defaults_config, inventory, authorizer):
    uid = os.geteuid()
    gid = os.getegid()

    # Set defaults
    for key, config in defaults_config.items():
        try:
            if read_only:
                myconf = config['readonly']
            else:
                myconf = config['fullauth']
        except KeyError:
            try:
                myconf = config['all']
            except KeyError:
                continue
        else:
            try:
                # security measure
                del config['fullauth']
            except KeyError:
                pass

        modname, clsname = key.split(':')
        module = __import__('dynamo.' + modname, globals(), locals(), [clsname])
        cls = getattr(module, clsname)

        cls.set_default(myconf)

    if is_local:
        os.chdir(path)
    else:
        # Confine in a chroot jail
        # Allow access to directories in PYTHONPATH with bind mounts
        for base in find_common_base(map(os.path.realpath, sys.path)):
            try:
                os.makedirs(path + base)
            except OSError:
                # shouldn't happen but who knows
                continue

            bindmount(base, path + base)

        os.mkdir(path + '/tmp')
        os.chmod(path + '/tmp', 0777)

        os.seteuid(0)
        os.setegid(0)
        os.chroot(path)

        path = ''
        os.chdir('/')

    # De-escalate privileges permanently
    os.seteuid(0)
    os.setegid(0)
    os.setgid(gid)
    os.setuid(uid)

    # We will react to SIGTERM by raising KeyboardInterrupt
    from dynamo.utils.signaling import SignalConverter

    signal_converter = SignalConverter()
    signal_converter.set(signal.SIGTERM)
    # we won't call unset()

    # Ignore SIGINT
    # If the main process was interrupted by Ctrl+C:
    # Ctrl+C will pass SIGINT to all child processes (if this process is the head of the
    # foreground process group). In this case calling terminate() will duplicate signals
    # in the child. Child processes have to always ignore SIGINT and be killed only from
    # SIGTERM sent by the line below.
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    # Reset logging
    reset_logger()

    # Pass my inventory and authorizer to the executable through core.executable
    import dynamo.core.executable as executable
    executable.inventory = inventory
    executable.authorizer = authorizer

    from dynamo.dataformat import Block
    Block._inventory_store = inventory._store

    if not read_only:
        executable.read_only = False
        # create a list of updated and deleted objects the executable can fill
        inventory._update_commands = []

    return path

def send_updates(inventory, queue, silent = False):
    if queue is None:
        return

    # Collect updates if write-enabled

    nobj = len(inventory._update_commands)
    if not silent:
        sys.stderr.write('Sending %d updated objects to the server process.\n' % nobj)
        sys.stderr.flush()
    wm = 0.
    for iobj, (cmd, objstr) in enumerate(inventory._update_commands):
        if not silent and float(iobj) / nobj * 100. > wm:
            sys.stderr.write(' %.0f%%..' % (float(iobj) / nobj * 100.))
            sys.stderr.flush()
            wm += 5.

        try:
            queue.put((cmd, objstr))
        except:
            if not silent:
                sys.stderr.write('Exception while sending %s %s\n' % (DynamoInventory._cmd_str[cmd], objstr))
                sys.stderr.flush()
            raise

    if not silent and nobj != 0:
        sys.stderr.write(' 100%.\n')
        sys.stderr.flush()
    
    # Put end-of-message
    queue.put((DynamoInventory.CMD_EOM, None))

    # Wait until all messages are received
    queue.join()

def post_execution(path, is_local):
    if not is_local:
        # jobs were confined in a chroot jail
        clean_remote_request(path)
