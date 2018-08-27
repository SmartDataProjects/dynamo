import os
import sys
import signal
import multiprocessing
import logging
from ctypes import cdll

libc = cdll.LoadLibrary("/lib64/libc.so.6") # will use glibc mount()

from dynamo.core.inventory import DynamoInventory
from dynamo.utils.log import log_exception
from dynamo.utils.path import find_common_base

BANNER = '''
+++++++++++++++++++++++++++++++++++++
++++++++++++++ DYNAMO +++++++++++++++
++++++++++++++  v2.2  +++++++++++++++
+++++++++++++++++++++++++++++++++++++
'''

def killproc(proc, LOG, timeout = 30):
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
