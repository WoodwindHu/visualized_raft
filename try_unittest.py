#!/usr/bin/python
# -*- coding: utf-8 -*-

import unittest


class TestDemo(unittest.TestCase):
    """Test mathfuc.py"""

    @classmethod
    def setUpClass(cls):
        print ("this setupclass() method only called once.\n")

    @classmethod
    def tearDownClass(cls):
        print ("this teardownclass() method only called once too.\n")

    def setUp(self):
        print ("do something before test : prepare environment.\n")

    def tearDown(self):
        print ("do something after test : clean up.\n")

    def test_add(self):
        """Test method add(a, b)"""
        self.assertEqual(3, 1+2)
        self.assertNotEqual(3, 2+2)

    def test_minus(self):
        """Test method minus(a, b)"""
        self.assertEqual(2, 3-1)
        self.assertNotEqual(1, 3-1)

    @unittest.skip("do't run as not ready")
    def test_minus_with_skip(self):
        """Test method minus(a, b)"""
        self.assertEqual(1, 3-2)
        self.assertNotEqual(1, 3-1)


if __name__ == '__main__':
    # verbosity=*：默认是1；设为0，则不输出每一个用例的执行结果；2-输出详细的执行结果
    unittest.main(verbosity=1)