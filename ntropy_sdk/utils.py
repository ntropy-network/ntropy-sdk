import math
from functools import singledispatch, update_wrapper


def assert_type(value, name, expected_type):
    if not isinstance(value, expected_type):
        raise TypeError(f"{name} should be of type {expected_type}")

    if expected_type == float or (
        isinstance(expected_type, tuple) and float in expected_type
    ):
        if math.isnan(value):
            raise ValueError(f"{name} value cannot be NaN")

    return True


# Ported from CPython implementation, starting from Python 3.8.0
# https://github.com/python/cpython/blob/main/Lib/functools.py
class singledispatchmethod:
    """Single-dispatch generic method descriptor.
    Supports wrapping existing descriptors and handles non-descriptor
    callables as instance methods.
    """

    def __init__(self, func):
        if not callable(func) and not hasattr(func, "__get__"):
            raise TypeError(f"{func!r} is not callable or a descriptor")

        self.dispatcher = singledispatch(func)
        self.func = func

    def register(self, cls, method=None):
        """generic_method.register(cls, func) -> func
        Registers a new implementation for the given *cls* on a *generic_method*.
        """
        return self.dispatcher.register(cls, func=method)

    def __get__(self, obj, cls=None):
        def _method(*args, **kwargs):
            method = self.dispatcher.dispatch(args[0].__class__)
            return method.__get__(obj, cls)(*args, **kwargs)

        _method.__isabstractmethod__ = self.__isabstractmethod__
        _method.register = self.register
        update_wrapper(_method, self.func)
        return _method

    @property
    def __isabstractmethod__(self):
        return getattr(self.func, "__isabstractmethod__", False)
