# -*- coding: utf-8 -*-

import bson
import re
from .schema import Schema, Or, And, Optional, Default
from pymongo import MongoClient


class Pools(object):

    _default_name = None
    _pool_confs = {}
    _pools = {}

    @classmethod
    def get_connection(cls, name=None):
        name = cls.get_default_name(name)
        return cls._pools.setdefault(name, cls.connect(name))

    @classmethod
    def connect(cls, name):
        confs = cls.get_confs(cls.get_default_name(name))
        if confs.get('username'):
            uri = 'mongodb://%s:%s@%s:%s/%s' % (confs['username'],
                                                confs.get('password', ''),
                                                confs['host'],
                                                confs['port'],
                                                confs['db'])
        else:
            uri = 'mongodb://%s:%s/%s' % (confs['host'],
                                          confs['port'],
                                          confs['db'])
        kwargs = {}
        for kw in ('max_pool_size',
                   'connectTimeoutMS',
                   'socketTimeoutMS',
                   'waitQueueTimeoutMS',
                   'waitQueueMultiple'):
            if kw in confs:
                kwargs[kw] = confs[kw]

        return MongoClient(host=uri, **kwargs)

    @classmethod
    def disconnect(cls, name):
        if name not in cls._pools:
            return
        connection = cls._pools[name]
        del cls._pools[name]
        connection.disconnect()

    @classmethod
    def get_database(cls, name=None):
        name = cls.get_default_name(name)
        connection = cls.get_connection(name)
        confs = cls.get_confs(name)
        return connection[confs['db']]

    @classmethod
    def has_name(cls, name):
        return name in cls._pool_confs

    @classmethod
    def get_default_name(cls, name=None):
        if cls.has_name(name):
            return name
        return cls._default_name

    @classmethod
    def set_default_name(cls, name):
        if name in cls._pool_confs:
            cls._default_name = name
        else:
            raise KeyError(name)
        return name

    @classmethod
    def get_confs(cls, name=None):
        if name not in cls._pool_confs:
            raise KeyError(name)
        return cls._pool_confs.get(name, {})

    @classmethod
    def set_confs(cls, name, confs):
        cls._pool_confs[name] = confs
        if len(cls._pool_confs) == 1:
            cls.set_default_name(name)
        return confs

    @classmethod
    def del_confs(cls, name):
        if cls.has_name(name):
            del cls._pool_confs[name]
        if len(cls._pool_confs) == 1:
            cls.set_default_name(cls._pool_confs.keys()[0])


_schema_of_structure = Schema({
    #Optional('_id'): Or(object, Default(bson.ObjectId, force_value=True)),
    Or(
        And(lambda k: isinstance(k, Optional),
            lambda v: re.compile(r"^$|^[^.$]{1,1}[^\.]*$").match(v._schema)),
        And(basestring, lambda v: re.compile(r"^$|^[^.$]{1,1}[^\.]*$").match(v)),
        error="key must match regular expresion r\"^$|^[^.$]{1,1}[^\.]*$\""
    ): object,
})


_schema_of_indices = Schema(
    Optional([
        {
            'fields': [(basestring, object)],
            Optional(basestring): object,
        }
    ])
)


class _DocumentMetaClass(type):

    def __new__(cls, name, bases, attrs):
        if attrs.get('__abstract__'):
            return type.__new__(cls, name, bases, attrs)
        attrs['__abstract__'] = False

        for name, value in attrs.iteritems():
            if name in ('_id',
                        '_doc',
                        '_changed_doc',
                        '_removed_doc',
                        '_ex_doc',
                        '_schema',
                        '_in_db',
                        '_changed'):
                raise AttributeError("Please don't use reserved attribute name `%s`" % name)

        collection = attrs.get('__collection__')
        if not collection:
            raise AttributeError("Can't define a mongo document without specification of collection name.")
        elif u'$' in collection:
            raise AttributeError("Don't use \\$ character in collection name.")

        structure = attrs.get('structure')
        assert isinstance(structure, dict)
        if not structure:
            raise AttributeError("Can't define a mongo document without stucture")

        if '_id' in structure:
            valid_id = structure['_id']
            del structure['_id']
            structure[Optional('_id')] = valid_id
        for key, value in structure.iteritems():
            if isinstance(key, Optional) and key._schema == '_id':
                break
        else:
            structure[Optional('_id')] = Or(object, Default(bson.ObjectId))

        structure = _schema_of_structure.validate(structure)
        attrs['_schema'] = Schema(structure)
        attrs.setdefault('indices',
                         _schema_of_indices.validate(getattr(bases[0], 'indices', [])))

        base_options = getattr(bases[0], '__options__', None)
        options = base_options.copy() if base_options else {}
        options.update(attrs.get('__options__', {}))
        attrs['__options__'] = options

        return type.__new__(cls, name, bases, attrs)


class Document(object):
    __metaclass__ = _DocumentMetaClass
    __abstract__ = True

    def __init__(self, *args, **kwargs):
        if not args and '_id' not in kwargs:
            raise TypeError("You must specify _id or initializing doc to create a document.")

        self._id = None
        self._doc = {}
        self._changed_doc = {}
        self._removed_doc = {}
        self._changed = False
        self._in_db = False

        if args[0]:
            if isinstance(args[0], dict):
                self._doc = self.validate(args[0])
                self._blur(changed_doc=self._doc)
            else:
                self._id = self.validate_id(args[0])

        elif '_id' in kwargs:
            self._id = self.validate_id(kwargs['_id'])

        if self._id:
            self.refresh()

    @classmethod
    def validate_id(cls, _id):
        s = Schema({k: v for k, v in cls._schema._schema.iteritems()
                    if (k == '_id'
                        or (isinstance(k, Optional)
                            and k._schema == '_id'))
                    })
        return s.validate({'_id': _id})['_id']

    @classmethod
    def validate_partial(cls, doc):
        assert isinstance(doc, dict)
        s = Schema({k: v for k, v in cls._schema._schema.iteritems()
                    if (k in doc
                        or (isinstance(k, Optional)
                            and k._schema in doc))
                    })
        return s.validate(doc)

    @classmethod
    def validate(cls, doc):
        assert isinstance(doc, dict)
        return cls._schema.validate(doc)

    def _clean(self):
        self._changed_doc = {}
        self._removed_doc = {}
        self._changed = False

    def _blur(self, changed_doc=None, removed_fields=None):
        if removed_fields:
            for key in removed_fields:
                if key in self._changed_doc:
                    del self._changed_doc[key]
                self._removed_doc[key] = ""

        if changed_doc:
            self._changed_doc.update(changed_doc)
            for key in changed_doc.iterkeys():
                if key in self._removed_doc:
                    del self._removed_doc[key]

        self._changed = True

    def refresh(self):
        if self._id is None:
            raise KeyError("`_id` is None.")

        doc = self.collection.find_one({'_id': self._id})
        if doc is None:
            self._doc = {}
            self._in_db = False
            return

        self._doc = self.validate(doc)
        self._id = self.validate_id(self._doc['_id'])

        self._clean()
        self._in_db = True

    def __len__(self):
        return len(self._doc) if self._doc else 0

    def __contains__(self, key):
        return True if self._doc and key in self._doc else False

    @classmethod
    def get_collection(cls):
        return Pools.get_database()[cls.__collection__]

    @property
    def collection(self):
        return self.__class__.get_collection()

    def save(self, replace=True, refresh=False, *args, **kwargs):
        if not self._changed:
            return

        if replace or not self._in_db:
            self._doc = self.validate(self._doc)
            _id = self.collection.save(self._doc, *args, **kwargs)
            self._id = self.validate_id(_id)

        elif self._changed:
            if self._removed_doc:
                self.collection.update({'_id': self._id}, {'$unset': self._removed_doc}, *args, **kwargs)

            _changed_doc = self.validate_partial(self._changed_doc)
            if _changed_doc:
                self.collection.update({'_id': self._id}, {'$set': _changed_doc}, *args, **kwargs)

        self._clean()
        self._in_db = True

        if refresh:
            self.refresh()
        return self._id

    def commit(self, refresh=False, *args, **kwargs):
        return self.save(replace=False, refresh=refresh, *args, **kwargs)

    def get(self, key, default=None):
        if not isinstance(key, tuple):
            return self._doc.get(key, default)
        return {k: self._doc.get(k, default) for k in key}

    def __getitem__(self, key):
        return self.get(key)

    def __delitem__(self, key):
        keys = (key,) if not isinstance(key, tuple) else key
        for name in keys:
            if name in self._doc:
                del self._doc[name]
        self._blur(removed_fields=keys)

    def __setitem__(self, key, value):
        self.update({key: value})

    def update(self, doc):
        doc = self.validate_partial(doc)
        self._doc.update(doc)
        self._blur(changed_doc=doc)

    def remove(self, *args, **kwargs):
        if not self._in_db:
            return
        self.collection.remove({'_id': self._id}, *args, **kwargs)
        self._clean()
        self._changed = True
        self._in_db = False

    @classmethod
    def ensure_indices(cls):
        if not cls._indices:
            return

        collection = cls.collection
        for index in cls._indices:
            fields = cls._indices['fields']
            kwargs = {key: value for key, value in cls._indices.iteritems()
                      if key != 'fields'}
            collection.ensure_index(fields, **kwargs)
