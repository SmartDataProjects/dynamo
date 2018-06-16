import sys
import logging
import traceback

def log_exception(logger):
    """
    Print the exception and traceback into the given logger.
    """

    exc_type, exc, tb = sys.exc_info()
    logger.critical('Caught an exception of type %s\n', exc_type.__name__)

    # Use a non-formatting formatter
    direct = logging.Formatter(fmt = '%(message)s')
    formatters = {}
    # Need to replace all formatters of the parents of this logger
    parent = logger
    while parent is not None:
        for handler in parent.handlers:
            formatters[handler] = handler.formatter
            handler.setFormatter(direct)

        parent = parent.parent

    logger.critical('Traceback (most recent call last):')
    logger.critical(''.join(traceback.format_tb(tb)))
    logger.critical('%s: %s', exc_type.__name__, str(exc))

    # Set back to the original formatters
    parent = logger
    while parent is not None:
        for handler in parent.handlers:
            handler.setFormatter(formatters[handler])

        parent = parent.parent

def reset_logger():
    # This is a rather hacky solution relying perhaps on the implementation internals of
    # the logging module. It might stop working with changes to the logging module.
    # The assumptions are:
    #  1. All loggers can be reached through Logger.manager.loggerDict
    #  2. The only operation logging.shutdown() does is to call flush() and close() over
    #     all handlers (i.e. calling the two functions is enough to ensure clean cutoff
    #     from all resources)
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
