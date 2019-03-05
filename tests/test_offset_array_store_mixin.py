from rolling_zarr_array_store import OffsetArrayStoreMixin
import json
from itertools import product


class Store(OffsetArrayStoreMixin, dict):
    pass


def test_negative_keys():
    store = Store(cache_offset=False)
    for i in range(0, 10):
        store[str(i)] = i

    offset = [5]
    store['.zattrs'] = json.dumps({'_offset': offset})

    assert store['-5'] == 0
    assert store['0'] == 5
    assert store['4'] == 9

    store = Store(cache_offset=False)
    offset = [-5]
    store['.zattrs'] = json.dumps({'_offset': offset})

    for i in range(0, 10):
        store[str(i)] = i

    assert store['0'] == 0
    assert store['5'] == 5
    assert store['9'] == 9


def test_read_write_no_offset():
    store = Store()
    for i in range(10):
        store[str(i)] = i

    for i in range(10):
        assert store[str(i)] == i


def test_cache_offset():
    store = Store()

    store['0'] = 0

    offset = [1]
    store['.zattrs'] = json.dumps({'_offset': offset})

    assert store['0'] == 0


def test_no_cache_offset():
    store = Store(cache_offset=False)

    store['0'] = 0
    store['1'] = 1

    assert store['0'] == 0

    offset = [1]
    store['.zattrs'] = json.dumps({'_offset': offset})

    assert store['0'] == 1


def test_read_write_with_offset():

    store = Store()
    offset = [1, 2]
    store['.zattrs'] = json.dumps({'_offset': offset})

    indexes = list(product(range(3), range(5)))
    keys = [str(i)+'.'+str(j) for i, j in indexes]
    offset_keys = [str(i+offset[0])+'.'+str(j+offset[1]) for i, j in indexes]

    for i, key in enumerate(keys):
        store[key] = offset_keys[i]

    for i, key in enumerate(keys):
        assert store[key] == offset_keys[i]

    for key, val in store.items():
        if not key.endswith('.zattrs'):
            assert key == val


def test_read_write_negative_offset():

    store = Store()
    offset = [-1, -2]
    store['.zattrs'] = json.dumps({'_offset': offset})

    indexes = list(product(range(3, 5), range(5, 6)))
    keys = [str(i)+'.'+str(j) for i, j in indexes]
    offset_keys = [str(i+offset[0])+'.'+str(j+offset[1]) for i, j in indexes]

    for i, key in enumerate(keys):
        store[key] = offset_keys[i]

    for i, key in enumerate(keys):
        assert store[key] == offset_keys[i]

    for key, val in store.items():
        if not key.endswith('.zattrs'):
            assert key == val


def test_read_write_with_prefixes():

    store = Store()
    offset = [-1, 2]
    prefix = 'some/prefix/or/other/'
    store[prefix+'.zattrs'] = json.dumps({'_offset': offset})

    indexes = list(product(range(3, 5), range(5, 6)))
    keys = [prefix+str(i)+'.'+str(j) for i, j in indexes]
    offset_keys = [prefix+str(i+offset[0])+'.'+str(j+offset[1]) for i, j in indexes]

    for i, key in enumerate(keys):
        store[key] = offset_keys[i]

    for i, key in enumerate(keys):
        assert store[key] == offset_keys[i]

    for key, val in store.items():
        if not key.endswith('.zattrs'):
            assert key == val


def test_read_write_diff_zattrs_prefix():

    store = Store()
    offset = [-1, 2]
    prefix = 'random/prefix/'
    store['.zattrs'] = json.dumps({'_offset': offset})
    store['other/prefix/.zattrs'] = json.dumps({'_offset': offset})

    indexes = list(product(range(3, 5), range(5, 6)))
    keys = [prefix+str(i)+'.'+str(j) for i, j in indexes]
    offset_keys = keys

    for i, key in enumerate(keys):
        store[key] = offset_keys[i]

    for i, key in enumerate(keys):
        assert store[key] == offset_keys[i]

    for key, val in store.items():
        if not key.endswith('.zattrs'):
            assert key == val
