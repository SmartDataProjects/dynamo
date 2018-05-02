import sys
import logging
import traceback

class IntegrityError(Exception):
    """Exception to be raised when data integrity error occurs."""
    pass

class ObjectError(Exception):
    """Exception to be raised when object handling rules are violated."""
    pass

class ConfigurationError(Exception):
    """Exception to be when invalid configuration is detected."""
    pass
