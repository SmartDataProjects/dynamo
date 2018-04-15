import os
import sys
from ctypes import cdll

libc = cdll.LoadLibrary("/lib64/libc.so.6") # will use glibc mount()

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

    # glibc mount() requires mount-remount to have a read-only bind mount
    libc.mount(source, target, None, BIND, None)
    libc.mount(source, target, None, RDONLY | REMOUNT | BIND, None)

def umount(path):
    libc.umount(path)

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
