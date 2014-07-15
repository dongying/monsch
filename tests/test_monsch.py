# -*- coding: utf-8 -*-

import bson
import datetime
import re
import unittest
from monsch import Pools, Document, Default, Or, And, Optional, Use, SchemaError


connection_name = 'test'
connection_confs = {
    'host': 'localhost',
    'port': 27017,
    'db': 'test',
}
collection_name = 'test_collection'


class TestDoc(Document):
    __collection__ = collection_name

    structure = {
        Optional('_id'): bson.ObjectId,
        'name': Use(str),
        'price': Use(float),
        'version': And(Use(str),
                       lambda v: re.compile(r'^v\d+\.\d+\.\d+$').match(v)),
        'ctime': Or(datetime.datetime,
                    default=Default(datetime.datetime.now)),
        Optional('desc'): Use(str),
        'status': Or(And(Use(int),
                         Or(0, 1, 2)),
                     default=1),
        'groups': Or([Use(float)], default=['user']),
        'confs': {
            'type': Or('a', 'b', 'c'),
        },
        'counts': Or({'total': And(Use(int),
                                   lambda v: v >= 0)},
                     default={'total': 0})
    }


class MonschTestCase(unittest.TestCase):

    def setUp(self):
        Pools.set_confs(connection_name,
                        connection_confs)

    def tearDown(self):
        Pools.disconnect(connection_name)

    def test_create_doc(self):
        doc = TestDoc({
            'name': 'test1',
            'price': 0,
            'version': 'v0.0.1',
            'status': 2,
            'confs': {
                'type': 'a',
            },
        })
        assert doc['counts']['total'] == 0
        assert isinstance(doc['ctime'], datetime.datetime)

    def test_create_invalid_doc(self):
        with self.assertRaises(SchemaError):
            TestDoc({
                '_id': 123,
                'price': 'a',
                'status': '3',
                'groups': ['3'],
                'confs': None,
            })
        with self.assertRaises(SchemaError):
            TestDoc({
                'price': 'a',
                'status': '3',
                'groups': ['3'],
                'confs': None,
            })
        with self.assertRaises(SchemaError):
            TestDoc({
                'status': '3',
                'groups': ['3'],
                'confs': None,
            })
        with self.assertRaises(SchemaError):
            TestDoc({
                'groups': ['3'],
                'confs': None,
            })
        with self.assertRaises(SchemaError):
            TestDoc({
                'confs': None,
            })


if __name__ == '__main__':
    unittest.main()
