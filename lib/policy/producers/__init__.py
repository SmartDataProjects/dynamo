"""
Classes in this package produce dataset attributes (Dataset.attr).
The classes must provide two methods with the following signature:
  __init__(self, config)
  load(self, inventory)
and a list of strings
  produces
which indicates the names of the dataset attributes the load() function provides.
"""

import os

_producers = {} # {attribute name: class}

# Not a very elegant way of handling imports, but this way we don't have to know a priori what files and classes exist
_moddir = os.path.dirname(__file__)
for pyfile in os.listdir(_moddir):
    # Only import directories or .py files that don't start with an underscore
    if pyfile.startswith('_') or (not os.path.isdir(_moddir + '/' + pyfile) and not pyfile.endswith('.py')):
        continue

    modname = pyfile.replace('.py', '')
    # Line below equivalent to "import dynamo.policy.producers.{modname}"
    module = __import__('dynamo.policy.producers.' + modname, globals(), locals())

    # Pick up names (that don't start with _) from the imported module (or package) that has the 'load' and 'produces' attributes
    for name in dir(module):
        if name.startswith('_'):
            continue

        cls = getattr(module, name)

        if not hasattr(cls, 'load'):
            continue

        try:
            attr_names = cls.produces
        except AttributeError:
            continue

        for attr_name in attr_names:
            try:
                _producers[attr_name].append(cls)
            except KeyError:
                _producers[attr_name] = [cls]


def get_producers(attr_names, producers_config):
    """
    Get the mapping {attribute name: producer object} for the given list of attribute names and the configuration dictionary.
    @param attr_names       [dataset attribute name]
    @param poducers_config  {producer class name: Configuration}

    @return {attribute name: producer object}
    """

    producer_objects = {}

    for attr_name in attr_names:
        # Find the provider of each dataset attribute

        try:
            classes = _producers[attr_name]
        except KeyError:
            LOG.error('Attribute %s is not provided by any producers.', attr_name)
            raise ConfigurationError('Invalid attribute name')

        selected_cls = None

        # There can be multiple classes providing the same attribute. Producers_config should contain the config for one and only one
        for cls in classes:
            if cls.__name__ not in producers_config:
                continue

            if selected_cls is not None:
                LOG.error('Attribute %s is provided by two producers: [%s %s]', attr_name, producer_cls.__name__, cls.__name__)
                LOG.error('Please fix the configuration so that each dataset attribute is provided by a unique producer.')
                raise ConfigurationError('Duplicate attribute producer')

                selected_cls = cls

        if selected_cls is None:
            LOG.error('Attribute %s is not provided by any producer.', attr_name)
            LOG.error('Please fix the configuration so that each dataset attribute is provided by a unique producer.')
            raise ConfigurationError('Invalid attribute name')

        # Finally instantiate the selected class
        config = producers_config[selected_cls.__name__]
        producer_objects[attr_name] = selected_cls(config)

    return producer_objects
