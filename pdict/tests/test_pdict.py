# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, absolute_import, print_function

import unittest
import os
import string
import tempfile
import shutil
import types

import six

from pdict import pdict


class Test(unittest.TestCase):
    def setUp(self):
        unittest.TestCase.setUp(self)
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)
        unittest.TestCase.tearDown(self)

    def get_db_file(self):
        db_file = os.path.join(self.test_dir, 'test.db')
        if os.path.exists(db_file):
            os.remove(db_file)
        return db_file

    def test_header(self):
        d = pdict.create(self.get_db_file(), 100)
        self.assertEqual(d.capacity, 100)
        self.assertEqual(len(d), 0)
        self.assertEqual(d.isfrozen, False)
        d.close()

    def test_size(self):
        db_file = self.get_db_file()
        d = pdict.create(db_file, 100)
        d['k'] = 'v'
        d['k2'] = 'v2'
        self.assertEqual(len(d), 2)
        d.close()

        d = pdict.Pdict(db_file, read_only=False)
        self.assertEqual(len(d), 2)

        d['k3'] = 'v3'
        self.assertEqual(len(d), 3)
        d.close()

    def test_persistence(self):
        db_file = self.get_db_file()
        d = pdict.create(db_file, 100)
        d['k'] = 'v'
        d['k2'] = 'v2'
        d.close()

        d = pdict.Pdict(db_file)

        self.assertEqual(d.capacity, 100)
        self.assertEqual(len(d), 2)

        self.assertEqual('v', d['k'])
        self.assertEqual('v2', d['k2'])
        d.close()

    def test_contains(self):
        db_file = self.get_db_file()
        d = pdict.create(db_file, 100)
        d['k'] = 'v'
        d['k2'] = 'v2'
        d['võti'] = 'väärtus'
        d.close()

        d = pdict.Pdict(db_file)

        self.assertTrue('k' in d)
        self.assertTrue('k2' in d)
        self.assertTrue('võti' in d)

        self.assertFalse('random' in d)
        self.assertFalse('other' in d)
        d.close()

    def test_binary(self):
        db_file = self.get_db_file()
        d = pdict.create(db_file, 5)
        d['a'] = 'a'
        d['b'] = b'b'
        d[b'c'] = 'c'
        d.close()

        d = pdict.Pdict(db_file)

        self.assertEqual(d.capacity, 5)

        self.assertEqual('a', d['a'])
        self.assertEqual(b'b', d['b'])

        if six.PY3:
            self.assertNotEqual('b', d['b'])

        self.assertTrue(b'c' in d)
        d.close()

    def test_value_types(self):
        db_file = self.get_db_file()
        d = pdict.create(db_file, 100)
        d['1'] = 1
        d['2'] = 'abc'
        d['3'] = 1.12e+100
        d['4'] = [1, 2, 3]
        d.close()

        d = pdict.Pdict(db_file)
        self.assertEqual(1, d['1'])
        self.assertEqual('abc', d['2'])
        self.assertAlmostEqual(1.12e+100, d['3'], places=3)
        self.assertEqual([1, 2, 3], d['4'])
        d.close()

    def test_rw(self):
        d = pdict.create(self.get_db_file(), 100)
        self.assertEqual(d.capacity, 100)

        d['k'] = 'v'
        self.assertEqual('v', d['k'])
        d['k'] = 'vv'
        self.assertEqual('vv', d['k'])
        d['võti'] = 'väärtus'
        self.assertEqual('väärtus', d['võti'])

        self.assertEqual(d.capacity, 100)

        d['k2'] = 'v2'
        self.assertEqual('v2', d['k2'])

        self.assertEqual(d.capacity, 100)

        self.assertIsNone(d.get('missing key'))
        self.assertRaises(KeyError, lambda: d['missing key'])
        d.close()

    def test_collisions(self):
        kvs = [(k, ord(k)) for k in (string.ascii_letters + string.digits)]
        d = pdict.create(self.get_db_file(), 32)
        for k, v in kvs:
            d[k] = v
        for k, v in kvs:
            self.assertEqual(v, d[k])
        d.close()

    def test_iteration(self):
        d = pdict.create(self.get_db_file(), 10)
        kvs = [('a', 1), ('b', 2), ('a', 3)]
        for k, v in kvs:
            d[k] = v

        keys = d.keys()
        self.assertTrue(isinstance(keys, types.GeneratorType))
        keys = list(keys)
        self.assertTrue(len(keys), 2)
        self.assertEqual(set(keys), {'a', 'b'})

        keys = [k for k in d]
        self.assertTrue(len(keys), 2)
        self.assertEqual(set(keys), {'a', 'b'})

        values = d.values()
        self.assertTrue(isinstance(values, types.GeneratorType))
        values = list(values)
        self.assertTrue(len(values), 2)
        self.assertEqual(set(values), {2, 3})

        items = d.items()
        self.assertTrue(isinstance(items, types.GeneratorType))
        items = list(items)
        self.assertTrue(len(items), 2)
        self.assertEqual(set(items), {('b', 2), ('a', 3)})
        d.close()

    def test_close(self):
        db_file = self.get_db_file()
        # create in writable mode
        d = pdict.create(db_file, 10)
        d['a'] = 1
        self.assertRaises(RuntimeError, lambda: pdict.Pdict(db_file))
        d.close()

        # re-open in writable mode
        d = pdict.Pdict(db_file, read_only=False)
        self.assertEqual(len(d), 1)
        self.assertEqual(d['a'], 1)
        self.assertEqual(d.capacity, 10)
        d['b'] = 2
        self.assertRaises(RuntimeError, lambda: pdict.Pdict(db_file))
        d.close()

        # re-open in read only mode
        d = pdict.Pdict(db_file)
        self.assertEqual(len(d), 2)
        self.assertEqual(d['a'], 1)
        self.assertEqual(d['b'], 2)
        d2 = pdict.Pdict(db_file)
        d.close()
        d2.close()


if __name__ == "__main__":
    unittest.main()
