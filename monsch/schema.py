# -*- coding: utf-8 -*-


class SchemaError(Exception):
    """Error during Schema validation."""

    def __init__(self, autos, errors):
        self.autos = autos if type(autos) is list else [autos]
        self.errors = errors if type(errors) is list else [errors]
        Exception.__init__(self, self.code)

    @property
    def code(self):
        def uniq(seq):
            seen = set()
            seen_add = seen.add
            return [x for x in seq if x not in seen and not seen_add(x)]
        a = uniq(i for i in self.autos if i is not None)
        e = uniq(i for i in self.errors if i is not None)
        if e:
            return '\n'.join(e)
        return '\n'.join(a)


class Strategy(object):

    def __init__(self, *args, **kw):
        assert set(kw.keys()).issubset(set(['error', 'default']))
        self._error = kw.get('error')
        if 'default' in kw:
            self.default = kw.get('default')

    def validate(self, data):
        raise NotImplementedError


class And(Strategy):

    def __init__(self, *args, **kw):
        self._args = args
        super(And, self).__init__(*args, **kw)

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__,
                           ', '.join(repr(a) for a in self._args))

    def validate(self, data):
        for s in [Schema(s, error=self._error) for s in self._args]:
            data = s.validate(data)
        return data


class Or(And):

    def validate(self, data):
        x = SchemaError([], [])
        for s in [Schema(s, error=self._error) for s in self._args]:
            try:
                return s.validate(data)
            except SchemaError as _x:
                x = _x
        raise SchemaError(['%r did not validate %r' % (self, data)] + x.autos,
                          [self._error] + x.errors)


class Use(Strategy):

    def __init__(self, callable_, **kw):
        assert callable(callable_)
        self._callable = callable_
        super(Use, self).__init__(**kw)

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self._callable)

    def validate(self, data):
        try:
            return self._callable(data)
        except SchemaError as x:
            raise SchemaError([None] + x.autos, [self._error] + x.errors)
        except BaseException as x:
            f = self._callable.__name__
            raise SchemaError('%s(%r) raised %r' % (f, data, x), self._error)


class Default(Strategy):

    def __init__(self, value, force_value=False, **kw):
        self._value = value
        self._force_value = force_value
        super(Default, self).__init__(**kw)

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self._value)

    def validate(self, *args):
        try:
            if callable(self._value) and not self._force_value:
                return self._value()
            else:
                return self._value
        except BaseException as x:
            raise SchemaError('Default raised %r' % x, self._error)


def priority(s):
    """Return priority for a give object."""
    if type(s) in (list, tuple, set, frozenset):
        return 6
    if type(s) is dict:
        return 5
    if isinstance(s, Strategy):
        return 4
    if issubclass(type(s), type):
        return 3
    if callable(s):
        return 2
    else:
        return 1


class Schema(Strategy):

    def __init__(self, schema, **kw):
        self._schema = schema
        if isinstance(schema, Strategy) and hasattr(schema, 'default'):
            self.default = schema.default
        super(Schema, self).__init__(**kw)

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self._schema)

    def validate(self, *args):
        if len(args) == 0:
            if not hasattr(self, 'default'):
                raise SchemaError('input missing for validation!', [self._error])
            if isinstance(self.default, Default):
                return self.default.validate()
            return self.default

        data = args[0]
        s = self._schema
        e = self._error

        if type(s) in (list, tuple, set, frozenset):
            data = Schema(type(s), error=e).validate(data)
            return type(s)(Or(*s, error=e).validate(d) for d in data)

        if type(s) is dict:
            data = Schema(dict, error=e).validate(data)
            new = type(data)()  # new - is a dict of the validated values
            x = None
            coverage = set()  # non-optional schema keys that were matched
            # for each key and value find a schema entry matching them, if any
            sorted_skeys = list(sorted(s, key=priority))
            for key, value in data.items():
                valid = False
                skey = None
                for skey in sorted_skeys:
                    svalue = s[skey]
                    try:
                        nkey = Schema(skey, error=e).validate(key)
                    except SchemaError:
                        pass
                    else:
                        try:
                            nvalue = Schema(svalue, error=e).validate(value)
                        except SchemaError as _x:
                            x = _x
                            raise
                        else:
                            coverage.add(skey)
                            valid = True
                            break
                if valid:
                    new[nkey] = nvalue
                elif skey is not None:
                    if x is not None:
                        raise SchemaError(['invalid value for key %r' % key] +
                                          x.autos, [e] + x.errors)

            coverage = set(k for k in coverage if type(k) is not Optional)
            required = set(k for k in s if type(k) is not Optional)

            for skey in required - coverage:
                if isinstance(skey, Strategy):
                    break
                svalue = s[skey]

                nkey = skey
                try:
                    nvalue = Schema(svalue, error=e).validate()
                except SchemaError:
                    break

                new[nkey] = nvalue
                coverage.add(skey)

            if coverage != required:
                raise SchemaError('missed keys %r' % (required - coverage), e)
            if len(new) < len(data):
                wrong_keys = set(data.keys()) - set(new.keys())
                s_wrong_keys = ', '.join('%r' % k for k in sorted(wrong_keys))
                raise SchemaError('wrong keys %s in %r' % (s_wrong_keys, data),
                                  e)
            return new

        if isinstance(s, Strategy):
            try:
                return s.validate(data)
            except SchemaError as x:
                raise SchemaError([None] + x.autos, [e] + x.errors)
            except BaseException as x:
                raise SchemaError('%r.validate(%r) raised %r' % (s, data, x),
                                  self._error)

        if issubclass(type(s), type):
            if isinstance(data, s):
                return data
            else:
                raise SchemaError('%r should be instance of %r' % (data, s), e)

        if callable(s):
            f = s.__name__
            try:
                if s(data):
                    return data
            except SchemaError as x:
                raise SchemaError([None] + x.autos, [e] + x.errors)
            except BaseException as x:
                raise SchemaError('%s(%r) raised %r' % (f, data, x),
                                  self._error)
            raise SchemaError('%s(%r) should evaluate to True' % (f, data), e)

        if s == data:
            return data

        else:
            raise SchemaError('%r does not match %r' % (s, data), e)


class Optional(Schema):

    """Marker for an optional part of Schema."""
