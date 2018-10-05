#! /usr/bin/env python

from dynamo import dataformat
from dynamo.core.inventory import DynamoInventory

CONF = dataformat.Configuration('/etc/dynamo/server_config.json')

def main():
    inv = DynamoInventory(CONF.inventory)


if __name__ == '__main__':
    main()
