#! /usr/bin/env python

from setup_inv import CONF

from dynamo import dataformat
from dynamo.core.inventory import DynamoInventory

def main(inventory=None):

    inv = inventory or DynamoInventory(CONF.inventory)
    inv.load()

    for attr in ['sites', 'datasets', 'groups']:
        for key, obj in getattr(inv, attr).items():
            if key is not None:
                inv.delete(obj)


if __name__ == '__main__':
    main()
