import code

from dynamo.core.executable import inventory

class DynamoConsole(code.InteractiveConsole):
    """
    Interactive console with inventory in the namespace.
    """

    def __init__(self, locals = None, filename = '<dynamo>'):
        if locals is None:
            mylocals = {"__name__": "__console__", "__doc__": None}
        else:
            mylocals = dict(locals)

        mylocals['inventory'] = inventory

        code.InteractiveConsole.__init__(self, locals = mylocals)
