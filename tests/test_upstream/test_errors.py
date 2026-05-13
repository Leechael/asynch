import pickle

import asynch.errors as err


def picklable(obj):
    picked = pickle.loads(pickle.dumps(obj))
    assert repr(obj) == repr(picked)
    assert str(obj) == str(picked)


def test_exception_picklable():
    picklable(err.Error("foo"))
    picklable(err.Error(message="foo"))

    picklable(err.ServerException("foo", 0, Exception()))
    picklable(err.ServerException(message="foo", code=0, nested=Exception()))
