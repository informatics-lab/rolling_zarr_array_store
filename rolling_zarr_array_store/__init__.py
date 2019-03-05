import re
import json
import s3fs

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

CHUNK_REGEX = re.compile("^-?[0-9]+([.]-?[0-9]+)*$")


def is_chunk(path):
    return bool(CHUNK_REGEX.match(path.split('/')[-1]))


class OffsetArrayStoreMixin(object):

    def __init__(self, *args, cache_offset=True, **kwargs):
        self._offsets = {}
        self.cache_offset = cache_offset
        super().__init__(*args, **kwargs)

    def __get_offset(self, prefix):
        offset = self._offsets.get(prefix, False) if self.cache_offset else False
        if offset is False:
            offset = None
            try:
                path = prefix + '/.zattrs' if prefix else '.zattrs'
                attrs = super().__getitem__(path)
                offset = json.loads(attrs).get('_offset', None)
            except KeyError:
                pass

            self._offsets[prefix] = offset
        return offset

    def __norm_path(self, path):
        if '/' in path:
            prefix, item = path.rsplit('/', 1)
        else:
            prefix = ''
            item = path
        return prefix, item

    def __apply_offset(self, path):
        prefix, item = self.__norm_path(path)
        if CHUNK_REGEX.match(item):
            offset = self.__get_offset(prefix)
            if offset:
                # Apply offset
                chunks = [int(i) for i in item.split('.')]
                offset_chunks = [chunk + offset[i] for i, chunk in enumerate(chunks)]
                item = '.'.join(str(v) for v in offset_chunks)

        path = prefix + '/' + item if prefix else item
        return path

    def __setitem__(self, path, item):
        return super().__setitem__(self.__apply_offset(path), item)

    def __getitem__(self, path):
        return super().__getitem__(self.__apply_offset(path))


class TempChunkTaggingArrayStore(s3fs.S3Map):

    def __init__(self, root, temp_chunk_path=None, check=False, anon=False):
        s3 = s3fs.S3FileSystem(anon=anon)
        self.temp_chunk_path = temp_chunk_path
        super().__init__(root, s3=s3, check=check)

    def __setitem__(self, path, item):
        if not is_chunk(path):
            return super().__setitem__(path, item)

        if self.temp_chunk_path is None or not path.rsplit('/', 1)[0].endswith(self.temp_chunk_path):
            return super().__setitem__(path, item)

        try:
            self.s3.s3_additional_kwargs['Tagging'] = 'type=temp_chunk'
            return super().__setitem__(path, item)
        finally:
            del self.s3.s3_additional_kwargs['Tagging']
