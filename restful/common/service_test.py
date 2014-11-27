#!/usr/bin/env python2
import time
import unittest

import audi
from audi.cdap import ClientRestClient
from audi.cdap import ServiceRestClient


class ServiceTest(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.app_id = 'PurchaseHistory'
        self.client = ClientRestClient()
        self.service = ServiceRestClient()
        self.service_ids = [
            'streams',
            'metrics',
            'appfabric',
            'transaction',
            'dataset.executor',
            'log.saver',
            'metrics.processor',
            'explore.service'
        ]

        # deploy app
        cdap_examples = audi.config['cdap']['examples']
        self.client.deploy_app(cdap_examples['Purchase'])

    @classmethod
    def tearDownClass(self):
        audi.stop_app(self.app_id)
        self.client.unrecoverable_reset()
        time.sleep(5)

    def test_service_status(self):
        # check individual service status
        for service in self.service_ids:
            resp = self.service.system_service_status(service)
            self.assertEquals(resp.status_code, 200)
            self.assertEquals(resp.json()['status'], 'OK')

        # check all service statuses
        resp = self.service.system_services_status()
        self.assertEquals(len(resp.json()), len(self.service_ids))
        self.assertEquals(resp.status_code, 200)

    def test_service_instances(self):
        # check individual service instances
        for service in self.service_ids:
            resp = self.service.system_service_instances(service)
            self.assertEquals(resp.status_code, 200)
            self.assertEquals(resp.json()['provisioned'], 1)
            self.assertEquals(resp.json()['requested'], 1)
