"""
A module used for communication between the server and the executables.
Also can be used to set up an environment for the executables to run
as standalone python scripts.
"""

from core.inventory import ObjectRepository

registry = None
inventory = ObjectRepository()
