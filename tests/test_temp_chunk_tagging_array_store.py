import rolling_zarr_array_store
from unittest.mock import patch, MagicMock
import copy


def test_init():
    store = rolling_zarr_array_store.TempChunkTaggingArrayStore('some/path', anon=True)


@patch('s3fs.S3FileSystem')
def test_temp_path_write(s3fs):
    temp_path = "is_temp"
    s3_args = {'my': 'arg'}
    expected_s3_args = {}

    def check_args(*args):
        assert s3_args == expected_s3_args

    s3fs.return_value.s3_additional_kwargs = s3_args
    s3fs.return_value.open.return_value.__enter__.return_value.write.side_effect = check_args
    store = rolling_zarr_array_store.TempChunkTaggingArrayStore('some/path', temp_chunk_path=temp_path, anon=True)

    # shouldn't tag
    expected_s3_args = copy.copy(s3_args)
    store['0.4'] = 'thing'
    store['{temp_path}/.zattrs'] = 'stuff'

    # should tag
    expected_s3_args = copy.copy(s3_args)
    expected_s3_args['Tagging'] = 'type=temp_chunk'
    store[f'{temp_path}/0.4'] = 'thing'
    store[f'some/other/prefix/{temp_path}/2.1.4'] = 'again'


@patch('s3fs.S3FileSystem')
def test_no_temp_path_write(s3fs):
    temp_path = "is_temp"
    s3_args = {'my': 'arg'}
    expected_s3_args = copy.copy(s3_args)

    def check_args(*args):
        assert s3_args == expected_s3_args

    s3fs.return_value.s3_additional_kwargs = s3_args
    s3fs.return_value.open.return_value.__enter__.return_value.write.side_effect = check_args
    store = rolling_zarr_array_store.TempChunkTaggingArrayStore('some/path', temp_chunk_path=temp_path, anon=True)

    # shouldn't tag
    store['0.4'] = 'thing'
    store['{temp_path}/.zattrs'] = 'stuff'
    expected_s3_args['Tagging'] = 'type=temp_chunk'
    store[f'{temp_path}/0.4'] = 'thing'
    store[f'some/other/prefix/{temp_path}/2.1.4'] = 'again'
