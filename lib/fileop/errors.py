import errno

# For whatever reason, the following codes are missing from python errno
errno.ENOMEDIUM = 123
errno.EMEDIUMTYPE = 124
errno.ECANCELED = 125
errno.ENOKEY = 126
errno.EKEYEXPIRED = 127
errno.EKEYREVOKED = 128
errno.EKEYREJECTED = 129

# message, error code
msg_to_code = [
    ('performance marker', errno.ETIMEDOUT),
    ('Name or service not known', errno.EHOSTUNREACH),
    ('Connection timed out', errno.ETIMEDOUT),
    ('Operation timed out', errno.ETIMEDOUT),
    ('Idle Timeout', errno.ETIMEDOUT),
    ('end-of-file was reached', errno.EREMOTEIO),
    ('end of file occurred', errno.EREMOTEIO),
    ('SRM_INTERNAL_ERROR', errno.ECONNABORTED),
    ('Internal server error', errno.ECONNABORTED),
    ('was forcefully killed', errno.ECONNABORTED),
    ('operation timeout', errno.ETIMEDOUT),
    ('proxy expired', errno.EKEYEXPIRED),
    ('with an error 550 File not found', errno.ENOENT),
    ('ile exists and overwrite', errno.EEXIST),
    ('No such file', errno.ENOENT),
    ('SRM_INVALID_PATH', errno.ENOENT),
    ('The certificate has expired', errno.EKEYEXPIRED),
    ('The available CRL has expired', errno.EKEYEXPIRED),
    ('SRM Authentication failed', errno.EKEYREJECTED),
    ('SRM_DUPLICATION_ERROR', errno.EKEYREJECTED),
    ('SRM_AUTHENTICATION_FAILURE', errno.EKEYREJECTED),
    ('SRM_AUTHORIZATION_FAILURE', errno.EKEYREJECTED),
    ('Authentication Error', errno.EKEYREJECTED),
    ('SRM_NO_FREE_SPACE', errno.ENOSPC),
    ('digest too big for rsa key', errno.EMSGSIZE),
    ('Can not determine address of local host', errno.ENONET),
    ('Permission denied', errno.EACCES),
    ('System error in write', errno.EIO),
    ('File exists', errno.EEXIST),
    ('checksum do not match', errno.EIO),
    ('CHECKSUM MISMATCH', errno.EIO),
    ('gsiftp performance marker', errno.ETIMEDOUT),
    ('Could NOT load client credentials', errno.ENOKEY),
    ('Error reading host credential', errno.ENOKEY),
    ('File not found', errno.ENOENT),
    ('SRM_FILE_UNAVAILABLE', errno.ENOENT),
    ('Unable to connect', errno.ENETUNREACH),
    ('could not open connection to', errno.ENETUNREACH),
    ('user not authorized', errno.EACCES),
    ('Broken pipe', errno.EPIPE),
    ('limit exceeded', errno.EREMOTEIO),
    ('write denied', errno.EACCES),
    ('error in reading', errno.EIO),
    ('over-load', errno.EREMOTEIO),
    ('connection limit', errno.EMFILE)
]

def find_msg_code(msg):
    for m, c in msg_to_code:
        if m in msg:
            return c

    return None

# from FTS3 heuristics.cpp + some originals
irrecoverable_errors = set([
    errno.ENOENT,          # No such file or directory
    errno.EPERM,           # Operation not permitted
    errno.EACCES,          # Permission denied
    errno.EEXIST,          # Destination file exists
    errno.EISDIR,          # Is a directory
    errno.ENAMETOOLONG,    # File name too long
    errno.E2BIG,           # Argument list too long
    errno.ENOTDIR,         # Part of the path is not a directory
    errno.EFBIG,           # File too big
    errno.ENOSPC,          # No space left on device
    errno.EROFS,           # Read-only file system
    errno.EPROTONOSUPPORT, # Protocol not supported by gfal2 (plugin missing?)
    errno.ECANCELED,       # Canceled
    errno.EIO,             # I/O error
    errno.EMSGSIZE,        # Message too long
    errno.ENONET,          # Machine is not on the network
    errno.ENOKEY,          # Could not load key
    errno.EKEYEXPIRED,     # Key has expired
    errno.EKEYREJECTED,    # Key was rejected by service
    errno.ENETUNREACH      # Network is unreachable
])
