#! /usr/bin/env python

import unittest
import requests

import dynamo_setup
import dynamo_teardown


req = lambda request: requests.get('https://localhost.localdomain%s' % request,
                                     cert='/tmp/x509up_u500').json()

req_sites = lambda: req('/data/inventory/sites')

class TestWeb(unittest.TestCase):
    def setUp(self):
        # Make sure the response is empty first
        self.assertFalse(req_sites()['data'])
        dynamo_setup.shell()

    def tearDown(self):
        dynamo_teardown.shell()
        # Make sure the response is empty first
        self.assertFalse(req_sites()['data'])

    def test_sites(self):
        # Show that our filler site 'SITE' is in the datareturned
        self.assertTrue('SITE' in [site['name'] for site in req_sites()['data']])

    def test_blockreplicas(self):
        res = req('/data/inventory/blockreplicas')
        self.assertTrue('data' in res)

    def test_requestlist(self):
        res = req('/data/inventory/requestlist')
        self.assertTrue('data' in res)

    def test_subscriptions(self):
        res = req('/data/inventory/subscriptions')
        self.assertTrue('data' in res)


if __name__ == '__main__':
    unittest.main()
