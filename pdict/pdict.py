# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, absolute_import, print_function

import os
import io
import mmap
from struct import pack as struct_pack, unpack as struct_unpack

from six.moves import range
from mmh3 import hash64 as mmh3_hash64
from msgpack import packb as msgpack_packb, unpackb as msgpack_unpackb

HEADER_SIZE = 10
BUCKET_SIZE = 4


class MMapBucketArray(object):
    """
    Bucket array implemented as a memory mapped file.

    Parameters
    ----------
    fileobj : file
        database file object.
    n : int
        table capacity.
    read_only : bool
        initialize mmap in read-only or read/write mode.
    """

    def __init__(self, fileobj, n, read_only):
        self.file = mmap.mmap(fileobj.fileno(),
                              length=HEADER_SIZE + BUCKET_SIZE * n,
                              access=mmap.ACCESS_READ if read_only else mmap.ACCESS_WRITE)

    def __setitem__(self, i, value):
        self.file.seek(HEADER_SIZE + i * BUCKET_SIZE)
        self.file.write(struct_pack(b'<I', value))

    def __getitem__(self, i):
        self.file.seek(HEADER_SIZE + i * BUCKET_SIZE)
        return struct_unpack(b'<I', self.file.read(BUCKET_SIZE))[0]


class BaseDict(object):
    """
    Base class for persistent dictionary.

    Parameters
    ----------
    fname : str
        existing data file to open
    read_only : bool
        open file in read-only or read/write mode.
    buffering : int
        buffering parameter passed to io.open() method.
    """

    def __init__(self, fname, read_only, buffering):
        self.read_only = read_only
        self.buffering = buffering
        self.file = io.open(fname,
                            mode='rb' if read_only else 'r+b',
                            buffering=buffering)

        header = Header.read(fname)
        self.buckets = MMapBucketArray(self.file, header.capacity, read_only)
        self._capacity = header.capacity
        self._size = header.size
        self._frozen = header.isfrozen
        self._wasclosed = header.wasclosed

        if not header.wasclosed:
            self.close()
            raise RuntimeError("Database file was not properly closed or is corrupt.")

    def get(self, key, default=None):
        try:
            return self.__getitem__(key)
        except KeyError:
            return default

    def __contains__(self, item):
        try:
            self.__getitem__(item)
            return True
        except KeyError:
            return False

    def __len__(self):
        return self._size

    def __iter__(self):
        return self.keys()

    def keys(self):
        for skey, svalue in self.iter_records():
            yield deserialize(skey)

    def values(self):
        for skey, svalue in self.iter_records():
            yield deserialize(svalue)

    def items(self):
        for skey, svalue in self.iter_records():
            yield deserialize(skey), deserialize(svalue)

    @property
    def isfrozen(self):
        return self._frozen

    @property
    def wasclosed(self):
        return self._wasclosed

    @property
    def capacity(self):
        return self._capacity

    def close(self):
        if not self.read_only:
            # update header
            Header.write_size(self._size, self.file)
            Header.write_closed(True, self.file)
            # flush file
            os.fsync(self.file.fileno())
        self.file.close()


class Pdict(BaseDict):
    """Persistent mutable dictionary"""

    def __init__(self, fname, read_only=True, buffering=-1):
        super(Pdict, self).__init__(fname, read_only, buffering)
        if self.isfrozen:
            raise RuntimeError("Can't open frozen file '{}' with Pdict."
                               "Use FrozenDict instead.".format(fname))
        if read_only is False:
            Header.write_closed(False, self.file)

    def __setitem__(self, key, value):
        skey = serialize(key)
        svalue = serialize(value)

        keyhash = compute_hash(skey)
        bucket = keyhash % self._capacity

        # read bucket
        record_offset = self.buckets[bucket]

        # move to the end of file 
        f = self.file
        f.seek(0, os.SEEK_END)
        record_insertion_offset = f.tell()

        # write record
        f.write(struct_pack('<HH {}s {}s I'.format(len(skey), len(svalue)),
                            len(skey), len(svalue), skey, svalue, record_offset))
        # update bucket
        self.buckets[bucket] = record_insertion_offset
        self._size += 1

    def __getitem__(self, key):
        skey = serialize(key)
        keyhash = compute_hash(skey)
        bucket = keyhash % self._capacity
        record_offset = self.buckets[bucket]

        # search record
        f = self.file
        while record_offset != 0:
            f.seek(record_offset)
            key_len, value_len = struct_unpack(b'<HH', f.read(4))
            record_skey = f.read(key_len)
            if skey == record_skey:
                return deserialize(f.read(value_len))
            else:
                f.read(value_len)
                record_offset = struct_unpack(b'<I', f.read(4))[0]
        raise KeyError()

    def iter_records(self):
        f = self.file
        f.seek(HEADER_SIZE + self._capacity * BUCKET_SIZE)
        for bucket in range(self._capacity):
            rec_offset = self.buckets[bucket]
            bucket_keys = set()
            while rec_offset != 0:
                f.seek(rec_offset)
                key_len, val_len = struct_unpack(b'<HH', f.read(4))
                skey, svalue = f.read(key_len), f.read(val_len)
                rec_offset = struct_unpack(b'<I', f.read(4))[0]
                if skey not in bucket_keys:
                    yield skey, svalue
                    bucket_keys.add(skey)


class Header(object):
    """Database file header."""

    def __init__(self, capacity, size, isfrozen, wasclosed):
        self.capacity = capacity
        self.size = size
        self.isfrozen = isfrozen
        self.wasclosed = wasclosed

    @staticmethod
    def read(fname):
        with io.open(fname, 'rb') as f:
            fields = struct_unpack(b'<II??', f.read(HEADER_SIZE))
            return Header(*fields)

    def write(self, fileobj):
        fileobj.seek(0)
        fileobj.write(struct_pack(b'<IIbb',
                                  self.capacity, self.size, self.isfrozen, self.wasclosed))

    @staticmethod
    def write_capacity(capacity, fileobj):
        Header.write_field(0, b'<I', capacity, fileobj)

    @staticmethod
    def write_size(size, fileobj):
        Header.write_field(4, b'<I', size, fileobj)

    @staticmethod
    def write_frozen(isfrozen, fileobj):
        Header.write_field(8, b'<?', isfrozen, fileobj)

    @staticmethod
    def write_closed(closed, fileobj):
        Header.write_field(9, b'<?', closed, fileobj)

    @staticmethod
    def write_field(offset, fmt, value, fileobj):
        fileobj.seek(offset)
        fileobj.write(struct_pack(fmt, value))


def create(fname, capacity, *args, **kvargs):
    """Create a new empty dictionary"""
    if os.path.exists(fname):
        raise IOError('File "{}" already exists!'.format(fname))
    create_db_file(fname, capacity)
    return Pdict(fname, read_only=False, *args, **kvargs)


def create_db_file(fname, capacity, frozen=False):
    """Initialize a database file"""
    with io.open(fname, 'wb') as f:
        # init header
        Header(capacity, 0, frozen, True).write(f)
        # init bucket array
        empty_bucket_entry = struct_pack(b'<I', 0)
        for _ in range(capacity):
            f.write(empty_bucket_entry)


def serialize(value):
    return msgpack_packb(value, use_bin_type=True)


def deserialize(value):
    return msgpack_unpackb(value, raw=False)


def compute_hash(key):
    return mmh3_hash64(key)[0]
