#! /usr/bin/env python

import unittest

import dynamo_teardown

from dynamo import dataformat
from dynamo.core.executable import inventory

from dynamo.core.components.persistency import InventoryStore
from dynamo.core.components.impl.mysqlstore import MySQLInventoryStore
from dynamo.core.inventory import DynamoInventory


CONF = dataformat.Configuration('/etc/dynamo/server_config.json')

class TestEmpty(unittest.TestCase):
    # Test random stuff with empty databases
    def test_fresh(self):
        # No sites
        self.assertEqual(inventory.sites, {})
        # But define the null group always
        self.assertEqual(inventory.groups,
                         {None: dataformat.Group.null_group})

    def test_type(self):
        # Now MySQLInventoryStore can never be deleted
        mystore = MySQLInventoryStore(CONF.inventory.persistency.config)
        invstore = InventoryStore.get_instance('mysqlstore:MySQLInventoryStore',
                                               CONF.inventory.persistency.config)

        self.assertTrue(mystore.check_connection())
        self.assertTrue(invstore.check_connection())

        self.assertEqual(mystore.__class__, invstore.__class__)

    def test_connect(self):
        inv = DynamoInventory(CONF.inventory)

        self.assertTrue(inv.has_store)

        # Check the store type
        mystore = MySQLInventoryStore(CONF.inventory.persistency.config)
        self.assertEqual(mystore.__class__, inv._store.__class__)


class TestDynamoInventory(unittest.TestCase):
    def setUp(self):
        self.inv = DynamoInventory(CONF.inventory)
        # Make sure this is clear
        self.inv.load()
        self.assertEqual(self.inv.sites, {})
        self.assertEqual(self.inv.groups,
                         {None: dataformat.Group.null_group})

    def tearDown(self):
<<<<<<< HEAD
        teardown_inv.main(self.inv)
=======
        dynamo_teardown.main(self.inv)
>>>>>>> docker-test

    # Test things that need cleaning up after
    def test_addsite(self):
        good_site = dataformat.Site('GOOD_SITE')
        bad_site = dataformat.Site('BAD_SITE')

        self.inv.update(good_site)
        self.assertEqual(self.inv.sites,
                         {'GOOD_SITE': good_site})

        self.inv.update(bad_site)
        self.assertEqual(self.inv.sites,
                         {'GOOD_SITE': good_site,
                          'BAD_SITE': bad_site})

        # Start another connection:
        inv2 = DynamoInventory(CONF.inventory)
        # Need to do this to read from the database
        inv2.load()
        self.assertEqual(inv2.sites,
                         {'GOOD_SITE': good_site,
                          'BAD_SITE': bad_site})

    def test_updatesite(self):
        site = dataformat.Site('SITE')
        self.inv.update(site)

        inv2 = DynamoInventory(CONF.inventory)
        inv2.load()

        self.assertEqual(self.inv.sites['SITE'].status, dataformat.Site.STAT_UNKNOWN)
        self.assertEqual(inv2.sites['SITE'].status, dataformat.Site.STAT_UNKNOWN)

        site.status = dataformat.Site.STAT_READY

        self.inv.update(site)
        self.assertEqual(self.inv.sites['SITE'].status, dataformat.Site.STAT_READY)

        # Doesn't update if already loaded
        self.assertEqual(inv2.sites['SITE'].status, dataformat.Site.STAT_UNKNOWN)

        # Does update
        inv3 = DynamoInventory(CONF.inventory)
        inv3.load()
        self.assertEqual(inv3.sites['SITE'].status, dataformat.Site.STAT_READY)

        # Updates if reloaded
        inv2.load()
        self.assertEqual(inv2.sites['SITE'].status, dataformat.Site.STAT_READY)


if __name__ == '__main__':
    unittest.main()
