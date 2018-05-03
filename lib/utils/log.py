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
