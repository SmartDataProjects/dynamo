"""
A module used for communication between the server and the applications.
Also can be used to set up an environment for the applications to run
as standalone python scripts.
"""

from dynamo.core.inventory import ObjectRepository

## Whether this process has elevated privileges
authorized = False

## Handle to the inventory
inventory = ObjectRepository()

## Handle to the authorizer part of the master server
authorizer = None

## Make a standard logger that outputs to sys.stdout and sys.stderr
import sys
import logging

def make_standard_logger(level):
    log_level = getattr(logging, level.upper())
    log_format = '%(asctime)s:%(levelname)s:%(name)s: %(message)s'
    
    # Everything above log_level goes to stdout
    out_handler = logging.StreamHandler(sys.stdout)
    out_handler.setLevel(log_level)
    out_handler.setFormatter(logging.Formatter(fmt = log_format))
    # If >= ERROR, goes also to stderr
    err_handler = logging.StreamHandler(sys.stderr)
    err_handler.setLevel(logging.ERROR)
    err_handler.setFormatter(logging.Formatter(fmt = log_format))
    
    logger = logging.getLogger()
    logger.setLevel(log_level)
    logger.addHandler(out_handler)
    logger.addHandler(err_handler)

    return logger
