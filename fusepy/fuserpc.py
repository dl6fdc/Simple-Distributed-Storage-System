#!/usr/bin/env python

import xmlrpclib, pickle
from xmlrpclib import Binary

import logging

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

if not hasattr(__builtins__, 'bytes'):
    bytes = str

class Memory(LoggingMixIn, Operations):
    'Example memory filesystem. Supports only one level of files.'

    def __init__(self):
		# connect to server, the port is 51234. 
		# this hashtable is used to store file metadata
        self.files = xmlrpclib.ServerProxy("http://127.0.0.1:51234")

		# connect to server, the port is 54321.
		# the hashtable is used to store file data
        self.data = xmlrpclib.ServerProxy("http://127.0.0.1:54321")

        self.fd = 0
        now = time()
		
		# initiate the {'/': {st_nlink:2, st_ctime:now, ...} }
        value = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
                     st_mtime=now, st_atime=now, st_nlink=2)
        value = pickle.dumps(value)
        self.files.put(Binary('/'), Binary(value), 3000)


    def chmod(self, path, mode):
		# get the value
        rv = self.files.get(Binary(path))
        meta_dict = pickle.loads(rv["value"].data)
		# change
        meta_dict['st_mode'] &= 0770000
        meta_dict['st_mode'] |= mode
		# store changed value back
        value = pickle.dumps(meta_dict)
        self.files.put(Binary(path), Binary(value), 3000)

        return 0


    def chown(self, path, uid, gid):
		# get the valude
        rv = self.files.get(Binary(path))
        meta_dict = pickle.loads(rv["value"].data)
		# change
        meta_dict['st_uid'] = uid
        meta_dict['st_gid'] = gid
		# store back
        value = pickle.dumps(meta_dict)
        self.files.put(Binary(path), Binary(value), 3000)


    def create(self, path, mode):
		# store a new path/value, ie. a new file
        value = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                     st_size=0, st_ctime=time(), st_mtime=time(),
                     st_atime=time())
        value = pickle.dumps(value)
        self.files.put(Binary(path), Binary(value), 3000)

        self.fd += 1
        return self.fd


    def getattr(self, path, fh=None):
		# retrieve the value
        rv = self.files.get(Binary(path))
        if rv == {}:
            raise FuseOSError(ENOENT)
        else:
            meta_dict = pickle.loads(rv["value"].data)
        return meta_dict


    def getxattr(self, path, name, position=0):
		# retrieve
        rv = self.files.get(Binary(path))
        meta_dict = pickle.loads(rv["value"].data)
        attrs = meta_dict.get('attrs', {})

        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR


    def listxattr(self, path):
        rv = self.files.get(Binary(path))
        meta_dict = pickle.loads(rv["value"].data)
        attrs = meta_dict.get('attrs', {})

        return attrs.keys()

    def mkdir(self, path, mode):
		# put a new one
        value = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                     st_size=0, st_ctime=time(), st_mtime=time(),
                     st_atime=time())
        value = pickle.dumps(value)
        self.files.put(Binary(path), Binary(value), 3000)
		# change st_nlink
        rv = self.files.get(Binary('/'))
        meta_dict = pickle.loads(rv["value"].data)

        meta_dict['st_nlink'] += 1

        value = pickle.dumps(meta_dict)
        self.files.put(Binary('/'), Binary(value), 3000)


    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        rv = self.data.get(Binary(path))

        if rv == {}:
            value = rv.setdefault(path, '')
        else:
            value = pickle.loads(rv["value"].data)

        return value[offset:offset + size]

    def readdir(self, path, fh):
		# I changed the print_content():return self.data
		# so that I can get the keys
        contents = self.files.print_content()
        return ['.', '..'] + [x[1:] for x in contents if x != '/']

    def readlink(self, path):
        rv = self.data.get(Binary(path))

        if rv == {}:
            value = rv.setdefault(path, '')
        else:
            value = pickle.loads(rv["value"].data)

        return value

    def removexattr(self, path, name):
        rv = self.files.get(Binary(path))
        meta_dict = pickle.loads(rv["value"].data)
        attrs = meta_dict.get('attrs', {})

        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR

    def rename(self, old, new):
		# just insert the "new", leaving the "old" 
        rv = self.files.get(Binary(old))
        self.files.put(Binary(new), rv["value"], rv["ttl"])

    def rmdir(self, path):
		# the server don't have a delete method
        pass

    def setxattr(self, path, name, value, options, position=0):
        # Ignore options

        rv = self.files.get(Binary(path))
        meta_dict = pickle.loads(rv["value"].data)
        attrs = meta_dict.setdefault('attrs', {})

        attrs[name] = value

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        value = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
                     st_size=len(source))
        value = pickle.dumps(value)
        self.files.put(Binary(target), Binary(value), 3000)
        self.data[target] = source

    def truncate(self, path, length, fh=None):
		# retrieve the value
        rv = self.data.get(Binary(path))
        if rv == {}:
            value = rv.setdefault(path, '')
        else:
            value = pickle.loads(rv["value"].data)
        # truncate
        value = value[:length]
		# store back
        input_data = pickle.dumps(value)
        self.data.put(Binary(path), Binary(input_data), 3000)

		# change st_size
        rv2 = self.files.get(Binary(path))
        meta_dict = pickle.loads(rv2["value"].data)
        meta_dict['st_size'] = length

        value2 = pickle.dumps(meta_dict)
        self.files.put(Binary(path), Binary(value2), 3000)


    def unlink(self, path):
        pass

    def utimens(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)

        rv = self.files.get(Binary(path))
        meta_dict = pickle.loads(rv["value"].data)

        meta_dict['st_atime'] = atime
        meta_dict['st_mtime'] = mtime

        value = pickle.dumps(meta_dict)
        self.files.put(Binary(path), Binary(value), 3000)


    def write(self, path, data, offset, fh):
		# retrieve the value
        rv = self.data.get(Binary(path))
        if rv == {}:
            value = rv.setdefault(path, '')		# bytes = str
        else:
            value = pickle.loads(rv["value"].data)
		# write
        file_data = value[:offset] + data
		
        input_data = pickle.dumps(file_data)
        self.data.put(Binary(path), Binary(input_data), 3000)

		# change st_size
        rv2 = self.files.get(Binary(path))
        meta_dict = pickle.loads(rv2["value"].data)

        meta_dict['st_size'] = len(file_data)

        value2 = pickle.dumps(meta_dict)
        self.files.put(Binary(path), Binary(value2), 3000)

        return len(data)


if __name__ == '__main__':
    if len(argv) != 2:
        print('usage: %s <mountpoint>' % argv[0])
        exit(1)

    logging.getLogger().setLevel(logging.DEBUG)
    fuse = FUSE(Memory(), argv[1], foreground=True, debug=True)
