# -*- coding: utf-8 -*-

import bson
import datetime
import re
import unittest
from monsch import Pools, Document, Default, Or, And, Optional, Use


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
        'name': basestring,
        'price': Use(float),
        'version': And(Use(str),
                       lambda v: re.compile(r'^v\d+\.\d+\.\d+$').match(v)),
        'ctime': Or(datetime.datetime,
                    Default(datetime.datetime.now)),
        Optional('desc'): basestring,
        'status': Or(And(Use(int),
                         Or(0, 1, 2)),
                     Default(1)),
        'groups': Or([], Default(['user'])),
        'confs': {
            'type': Or('a', 'b', 'c'),
        },
        'counts': Or({'total': And(Use(int),
                                   lambda v: v >= 0)},
                     Default({'total': 0}))
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
            'confs': {
                'type': 'a',
            },
        })
        print repr(doc.__dict__)


if __name__ == '__main__':
    unittest.main()
