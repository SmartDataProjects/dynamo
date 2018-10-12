import time
import logging
import cStringIO
import gfal2

## Specify error codes that should not be considered as errors
from dynamo.fileop.errors import find_msg_code, irrecoverable_errors

LOG = logging.getLogger(__name__)

def gfal_exec(method, args, nonerrors = {}, return_value = False):
    """
    GFAL2 execution function
    @param method       Name of the Gfal2Context method to execute.
    @param args         Tuple of arguments to pass to the method
    @param nonerrors    Dictionary of error code translation for non-errors.
    @param return_value If True, simply return the return value of the function.

    @return  (exit code, start time, finish time, error message, log string)
    """

    start_time = None
    finish_time = None
    log = ''

    for attempt in xrange(5):
        # gfal2 knows to write to the logger. Redirect to StringIO and dump the full log at the end.
        stream = cStringIO.StringIO()
        LOG.handlers.pop()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(logging.Formatter(fmt = '%(asctime)s: %(message)s'))
        LOG.addHandler(handler)

        start_time = int(time.time())
    
        try:
            gfal2.set_verbose(gfal2.verbose_level.verbose)

            context = gfal2.creat_context()
            result = getattr(gfal2.Gfal2Context, method)(context, *args)

            finish_time = int(time.time())
        
        except gfal2.GError as err:
            if return_value:
                raise

            exitcode, msg = err.code, str(err)
            c = find_msg_code(msg)
            if c is not None:
                exitcode = c

            if exitcode in nonerrors:
                return 0, start_time, int(time.time()), nonerrors[exitcode], ''

            elif exitcode in irrecoverable_errors:
                break

        except Exception as exc:
            if return_value:
                raise

            exitcode, msg = -1, str(exc)
    
        else:
            exitcode, msg = 0, None
    
        finally:
            handler.flush()
            log_tmp = stream.getvalue().strip()

        # give a nice indent to each line
        log = ''.join('  %s\n' % line for line in log_tmp.split('\n'))
    
        stream.close()

        break

    if return_value:
        return result
    else:
        # all variables would be defined even when all attempts are exhausted
        return exitcode, start_time, finish_time, msg, log
