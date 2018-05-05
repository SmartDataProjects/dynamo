import os
import sys
import signal
import multiprocessing
import traceback
import shlex
import logging
from ctypes import cdll

libc = cdll.LoadLibrary("/lib64/libc.so.6") # will use glibc mount()

from dynamo.core.inventory import DynamoInventory

BANNER = '''
+++++++++++++++++++++++++++++++++++++
++++++++++++++ DYNAMO +++++++++++++++
++++++++++++++  v2.1  +++++++++++++++
+++++++++++++++++++++++++++++++++++++
'''

def killproc(proc, timeout = 5):
    try:
        proc.terminate()
    except OSError:
        pass
    proc.join(timeout)

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

def find_common_base(paths):
    """
    Find the "greatest common denominator" (shared base directories) of given paths. If the GCD of
    two paths is /, the two are considered to not share any base.
    @param paths    List of paths

    @return list of shared base directories.
    """

    if len(paths) < 2:
        return paths

    base_directories = [os.path.realpath(paths[0])]

    def compare_two(path1, path2):
        parts1 = path1.split('/')[1:] # [0] is ''
        parts2 = path2.split('/')[1:]

        ip = 0
        while ip < len(parts1) and ip < len(parts2):
            if parts1[ip] != parts2[ip]:
                break
            ip += 1

        return '/' + '/'.join(parts1[:ip])

    for path in paths[1:]:
        path = os.path.realpath(path)
        
        for ib, base in enumerate(list(base_directories)):
            common = compare_two(base, path)

            if common != '/':
                base_directories[ib] = common
                # no other entry in base_directories should have anything in common with path
                break

        else:
            # no entry had a common base
            base_directories.append(path)

    return base_directories

# Directories to bind-mount for read-only processes
mountpoints = find_common_base(sys.path)

def umountall(path):
    for mount in mountpoints:
        umount(path + mount)

def clean_remote_request(path):
    # Since threads cannot change the uid, we launch a subprocess.
    # (Mounts are made read-only, so there is no risk of accidents even if the subprocess fails)
    proc = multiprocessing.Process(target = umountall, args = (path,))
    proc.start()
    proc.join()

def run_script(path, args, is_local, defaults_config, inventory, authorizer, queue = None):
    """
    Main function for script execution.
    @param path            Path to the work area of the script. Will be the root directory in read-only processes.
    @param args            Script command-line arguments.
    @param is_local        True if script is requested from localhost.
    @param defaults_config A Configuration object specifying the global defaults for various tools
    @param inventory       A DynamoInventoryProxy instance
    @param authorizer      An Authorizer instance
    @param queue           Queue if write-enabled.
    """

    old_stdout = sys.stdout
    old_stderr = sys.stderr
    stdout = open(path + '/_stdout', 'a')
    stderr = open(path + '/_stderr', 'a')
    sys.stdout = stdout
    sys.stderr = stderr

    path = pre_execution(path, is_local, queue is None, defaults_config, inventory, authorizer)

    # Set argv
    sys.argv = [path + '/exec.py']
    if args:
        sys.argv += shlex.split(args) # split using shell-like syntax

    # Execute the script
    try:
        try:
            myglobals = {'__builtins__': __builtins__, '__name__': '__main__', '__file__': 'exec.py', '__doc__': None, '__package__': None}
            execfile(path + '/exec.py', myglobals)
        except SystemExit as exc:
            if exc.code == 0:
                pass
            else:
                raise
    except:
        # cut out the first block of traceback (which refers to this function)
        exc_type, exc, tb = sys.exc_info()
        tb_lines = traceback.format_tb(tb)[1:]
        sys.stderr.write('Traceback (most recent call last):\n')
        sys.stderr.write(''.join(tb_lines))
        sys.stderr.write('%s: %s\n' % (exc_type.__name__, str(exc)))
        sys.stderr.flush()
    finally:
        post_execution(path, is_local, inventory, queue)

    sys.stdout = old_stdout
    sys.stderr = old_stderr
    stdout.close()
    stderr.close()

    # Queue stays available on the other end even if we terminate the process

    return 0

def run_interactive(path, is_local, defaults_config, inventory, authorizer, make_console, stdout = sys.stdout, stderr = sys.stderr):
    """
    Main function for interactive sessions.
    For now we limit interactive sessions to read-only.
    @param path            Path to the work area.
    @param is_local        True if script is requested from localhost.
    @param defaults_config A Configuration object specifying the global defaults for various tools
    @param inventory       A DynamoInventoryProxy instance
    @param authorizer      An Authorizer instance
    @param make_console    A callable which takes a dictionary of locals as an argument and returns a console
    @param stdout          File-like object for stdout
    @param stderr          File-like object for stderr
    """

    old_stdout = sys.stdout
    old_stderr = sys.stderr
    sys.stdout = stdout
    sys.stderr = stderr

    pre_execution(path, is_local, True, defaults_config, inventory, authorizer)

    # use receive of oconn as input
    mylocals = {'__builtins__': __builtins__, '__name__': '__main__', '__doc__': None, '__package__': None, 'inventory': inventory}
    console = make_console(mylocals)
    try:
        console.interact(BANNER)
    finally:
        post_execution(path, is_local, inventory, None)

    sys.stdout = old_stdout
    sys.stderr = old_stderr

    return 0

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
            myconf = config['all']
        else:
            # security measure
            del config['fullauth']

        modname, clsname = key.split(':')
        module = __import__('dynamo.' + modname, globals(), locals(), [clsname])
        cls = getattr(module, clsname)

        cls.set_default(myconf)

    if is_local:
        os.chdir(path)
    else:
        # Confine in a chroot jail
        # Allow access to directories in PYTHONPATH with bind mounts
        for base in mountpoints:
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

    # Ignore SIGINT - see note above proc.terminate()
    # We will react to SIGTERM by raising KeyboardInterrupt
    from dynamo.utils.signaling import SignalConverter
    
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    signal_converter = SignalConverter()
    signal_converter.set(signal.SIGTERM)
    # we won't call unset()

    # Reset logging
    # This is a rather hacky solution relying perhaps on the implementation internals of
    # the logging module. It might stop working with changes to the logging.
    # The assumptions are:
    #  1. All loggers can be reached through Logger.manager.loggerDict
    #  2. All logging.shutdown() does is call flush() and close() over all handlers
    #     (i.e. calling the two is enough to ensure clean cutoff from all resources)
    #  3. root_logger.handlers is the only link the root logger has to its handlers
    for logger in [logging.getLogger()] + logging.Logger.manager.loggerDict.values():
        while True:
            try:
                handler = logger.handlers.pop()
            except AttributeError:
                # logger is just a PlaceHolder and does not have .handlers
                break
            except IndexError:
                break

            handler.flush()
            handler.close()

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

def post_execution(path, is_local, inventory, queue):
    if queue is not None:
        # Collect updates if write-enabled

        nobj = len(inventory._update_commands)
        sys.stderr.write('Sending %d updated objects to the server process.\n' % nobj)
        sys.stderr.flush()
        wm = 0.
        for iobj, (cmd, objstr) in enumerate(inventory._update_commands):
            if float(iobj) / nobj * 100. > wm:
                sys.stderr.write(' %.0f%%..' % (float(iobj) / nobj * 100.))
                sys.stderr.flush()
                wm += 5.

            try:
                queue.put((cmd, objstr))
            except:
                sys.stderr.write('Exception while sending %s %s\n' % (DynamoInventory._cmd_str[cmd], objstr))
                raise

        if nobj != 0:
            sys.stderr.write(' 100%.\n')
            sys.stderr.flush()
        
        # Put end-of-message
        queue.put((DynamoInventory.CMD_EOM, None))

    if not is_local:
        # jobs were confined in a chroot jail
        clean_remote_request(path)
