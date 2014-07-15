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
        'groups': Or([Use(str)], default=['user']),
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
        TestDoc.get_collection().drop()
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

    def test_save_doc(self):
        doc = TestDoc({
            'name': 'test1',
            'price': 0,
            'version': 'v0.0.1',
            'status': 2,
            'confs': {
                'type': 'a',
            },
        })
        doc.save()
        doc.refresh()
        print doc._id
        assert doc._in_db
        assert not doc._changed
        print doc._doc

    def test_commit(self):
        doc = TestDoc({
            'name': 'testcommit',
            'price': 0,
            'version': 'v0.0.1',
            'status': 2,
            'confs': {
                'type': 'a',
            },
            'desc': "heheho",
        })
        doc.save()
        doc.refresh()

        ctime = datetime.datetime.now()
        doc['ctime'] = ctime
        del doc['desc']
        assert doc._changed
        assert doc._removed_doc
        print doc._changed_doc
        print doc._removed_doc
        doc.commit()
        assert not doc._changed
        assert doc['ctime'] - ctime < datetime.timedelta(0, 0, 1000)

        doc.refresh()
        assert doc['ctime'] - ctime < datetime.timedelta(0, 0, 1000)
        getdoc = TestDoc(doc['_id'])
        assert getdoc['ctime'] - ctime < datetime.timedelta(0, 0, 1000)

    def test_remove(self):
        doc = TestDoc({
            'name': 'testremove',
            'price': 0,
            'version': 'v0.0.1',
            'status': 2,
            'confs': {
                'type': 'a',
            },
            'desc': "heheho",
        })
        doc.save()
        _id = doc._id

        assert TestDoc.get_collection().find_one({'_id': _id})
        doc.remove()
        assert not TestDoc.get_collection().find_one({'_id': _id})


if __name__ == '__main__':
    unittest.main()
