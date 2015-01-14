#!/usr/bin/env python2.7
#
# CDDL HEADER START
#
# The contents of this file are subject to the terms of the
# Common Development and Distribution License, version 1.1 (the "License").
# You may not use this file except in compliance with the License.
#
# You can obtain a copy of the license at ./LICENSE.
# See the License for the specific language governing permissions
# and limitations under the License.
#
# When distributing Covered Code, include this CDDL HEADER in each
# file and include the License file at ./LICENSE.
# If applicable, add the following below this CDDL HEADER, with the
# fields enclosed by brackets "[]" replaced with your own identifying
# information: Portions Copyright [yyyy] [name of copyright owner]
#
# CDDL HEADER END
#

# Copyright (c) 2015 Daniel W. Steinbrook. All rights reserved.

#
# ZzzFS strucutre:
#
#   <ZZZFS_ROOT>/
#     <pool_name>/
#       data -> <disk>
#       properties/
#       filesystems/
#         <fs_name>/
#           data -> ../data/<fs_name>/
#           properties/
#           snapshots/
#             <snapshot_name>/
#               data/
#               properties/
#             [...]
#         <fs_name>%<sub_fs_name>/
#           data -> ../data/<fs_name>/<sub_fs_name>/
#           properties/
#           snapshots/
#         [...]
#     [...]

import os
import csv
import pwd
import gzip
import shutil
import logging
import tarfile
import datetime
import platform
import cStringIO

from util import validate_component_name, ZzzFSException

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

ZZZFS_DEFAULT_ROOT = os.path.expanduser('~/.zzzfs')


def get_dataset_by(dataset_name, should_be=None, should_exist=True):
    '''Handle user-specified dataset name, returning a Filesystem or Snapshot
    based on the name. If should_be is specified, an exception is raised if the
    dataset is not an instance of the specified class. If should_exist is
    False/True, an exception is raised if the dataset does/does not already
    exist; no check is performed if should_exist is None.
    '''
    # validate dataset identifier
    filesystem_name = dataset_name
    snapshot_name = None
    # distinguish between "fs_name" and "fs_name@snapshot"
    if dataset_name.count('@') == 1:
        filesystem_name, snapshot_name = dataset_name.split('@', 1)

    if not validate_component_name(filesystem_name, allow_slashes=True):
        raise ZzzFSException, '%s: invalid dataset identifier' % dataset_name

    obj = Filesystem(dataset_name)
    if snapshot_name:
        if not validate_component_name(snapshot_name):
            raise ZzzFSException, '%s: invalid snapshot name' % snapshot_name

        obj = Snapshot(filesystem_name, snapshot_name)

    if should_be:
        if not isinstance(obj, should_be):
            raise ZzzFSException, '%s: not a %s' % (
                dataset_name, should_be.__name__.lower())

    if should_exist and not obj.exists():
        raise ZzzFSException, '%s: no such dataset' % dataset_name
    if obj.exists() and should_exist == False:
        raise ZzzFSException, '%s: dataset exists' % dataset_name

    # pool should exist, even if dataset itself shouldn't
    #logger.debug('%s, in pool %s', obj, obj.pool)
    if not obj.pool.exists():
        raise ZzzFSException, '%s: no such pool' % obj.pool.name

    return obj


class Dataset(object):
    '''Base class for Pool, Filesystem, and Snapshot. Contains methods that
    apply to all three objects.
    '''
    def __repr__(self):
        return '<%s: %s>' % (self.__class__.__name__, self.name)

    @property
    def properties(self):
        return os.path.join(self.root, 'properties')

    @property
    def data(self):
        return os.path.join(self.root, 'data')

    @property
    def base_attrs(self):
        return {'name': self.name}

    def get_parent(self):
        if '/' in self.name:
            return Filesystem(self.name.rsplit('/', 1)[-2])
        return Pool(self.name)

    def get_local_properties(self):
        attrs = self.base_attrs
        if not os.path.exists(self.properties):
            # no local attributes
            return attrs

        for key in os.listdir(self.properties):
            with open(os.path.join(self.properties, key), 'r') as f:
                attrs[key] = f.read()

        #logger.debug('%s local attributes: %s', self.name, attrs)
        return attrs

    def get_inherited_properties(self):
        attrs = {}
        local_attrs = self.get_local_properties()

        # inherit values for any attributes not overridden locally, bottom-up
        parent = self
        while parent.get_parent():
            parent = parent.get_parent()
            for key, val in parent.get_local_properties().iteritems():
                if key not in attrs and key not in local_attrs:
                    attrs[key] = val

        return attrs

    def add_local_property(self, key, val):
        if not os.path.exists(self.properties):
            os.makedirs(self.properties)
        if '/' in key:
            raise ZzzFSException, '%s: invalid property' % key
        with open(os.path.join(self.properties, key), 'w') as f:
            f.write(val)

    def get_property_and_source(self, key):
        local = self.get_local_properties()
        if key in local:
            return (local[key], 'local')

        inherited = self.get_inherited_properties()
        if key in inherited:
            return (inherited[key], 'inherited')

        # property not found
        return (None, None)

    def get_property(self, key):
        val, _ = self.get_property_and_source(key)
        return val


class Pool(Dataset):
    def __init__(self, name, should_exist=None):
        self.name = name

        zzzfs_root = os.environ.get('ZZZFS_ROOT', ZZZFS_DEFAULT_ROOT)
        if not os.path.exists(zzzfs_root):
            os.makedirs(zzzfs_root)

        self.root = os.path.join(zzzfs_root, self.name)
        self.filesystems = os.path.join(self.root, 'filesystems')
        self.history = os.path.join(self.root, 'history')

        if should_exist and not self.exists():
            raise ZzzFSException, '%s: no such pool' % self.name
        if should_exist == False and self.exists():
            raise ZzzFSException, '%s: pool exists' % self.name

    def get_parent(self):
        # pool is the top-most desendent of any dataset
        return None

    @classmethod
    def all(self):
        # return an array of all Pool objects
        try:
            return [Pool(name) for name in os.listdir(
                os.environ.get('ZZZFS_ROOT', ZZZFS_DEFAULT_ROOT))]
        except OSError:
            # zzzfs_root doesn't exist, so no pools have been created
            return []

    def exists(self):
        return self.name in os.listdir(
            os.environ.get('ZZZFS_ROOT', ZZZFS_DEFAULT_ROOT))

    def create(self, disk):
        if os.path.exists(disk) and len(os.listdir(disk)) != 0:
            raise ZzzFSException, '%s: disk in use' % self.name

        os.makedirs(self.root)
        pool_target = os.path.join(os.path.abspath(disk), self.name)
        os.makedirs(pool_target)
        os.symlink(pool_target, self.data)

        # create initial root filesystem for this pool
        Filesystem(self.name).create()

    def destroy(self):
        if not os.path.exists(os.path.realpath(self.data)):
            logger.warning('%s: pool has already been destroyed', self.name)
        else:
            shutil.rmtree(os.path.realpath(self.data))
        shutil.rmtree(self.root)

    def get_filesystems(self):
        # unescape slashes when instantiating Filesystem object
        return [Filesystem(x.replace('%', '/'))
            for x in os.listdir(self.filesystems)]

    def get_history(self, long_format=False):
        try:
            with open(self.history, 'rb') as f:
                history = csv.reader(f)
                for (date, command, user, host) in history:
                    if long_format:
                        yield '%s %s [user %s on %s]' % (date, command, user, host)
                    else:
                        yield '%s %s' % (date, command)

        except IOError:
            # no logged history
            pass

    def log_history_event(self, argv, date=None, user=None, host=None):
        command = ' '.join(argv)
        if not date:  # default date is now
            date = datetime.datetime.now()
        if not user:  # default user is user executing this script
            user = pwd.getpwuid(os.getuid()).pw_name
        if not host:  # default host is the current platform host
            host = platform.node()

        with open(self.history, 'a') as f:
            history = csv.writer(f)
            history.writerow(
                [date.strftime('%Y-%m-%d.%H:%M:%S'), command, user, host])


class Filesystem(Dataset):
    def __init__(self, filesystem):
        # need to escape slashes to use filesystem name as file name
        self.name = filesystem
        self.safe_name = self.name.replace('/', '%')

        # get pool name by walking up tree
        obj = self
        while obj.get_parent():
            obj = obj.get_parent()
        self.pool = Pool(obj.name)
        self.poolless_name = self.name[len(self.pool.name)+1:]

        self.root = os.path.join(self.pool.root, 'filesystems', self.safe_name)
        self.snapshots = os.path.join(self.root, 'snapshots')

    @property
    def mountpoint(self):
        # before the filesystem is created, the symlink doesn't resolve, so
        # this is a method that recomputes te property whenever it is accessed
        return os.path.realpath(self.data)

    @property
    def base_attrs(self):
        data = super(Filesystem, self).base_attrs
        data['mountpoint'] = self.mountpoint
        return data

    def exists(self):
        return os.path.exists(self.root)

    def create(self, from_stream=None):
        # create relative symlink into pool data
        target = os.path.join('..', '..', 'data', self.poolless_name)
        try:
            os.makedirs(os.path.join(self.root, target))
        except OSError:
            # already exists
            pass
        os.symlink(target, self.data)
        os.makedirs(self.properties)
        os.makedirs(self.snapshots)
        #logger.debug('%s: pointed %s at %s', self, self.data, target)

        if from_stream:
            # for receive command: inverse of Snapshot.to_stream
            try:
                # gzip needs a seekable object, not a stream
                #XXX this entails fitting the entire snapshot itno memeory
                buf = cStringIO.StringIO(from_stream.read())
                buf.seek(0)
                with gzip.GzipFile(fileobj=buf) as g:
                    with tarfile.TarFile(fileobj=g) as t:
                        #logger.debug('files in stream: %s', t.getnames())
                        # extract into snapshots directory
                        t.extractall(self.snapshots)

                        # "rollback" filesystem to snapshot just received
                        self.rollback_to(
                            Snapshot(self.name, os.listdir(self.snapshots)[0]))

            except Exception, e:
                # if anything goes wrong, destroy target filesystem and exit
                self.destroy()
                raise ZzzFSException, e

        #logger.debug(
        #    'after creating %s, filesystems in %s: %s', self, self.pool,
        #    self.pool.get_filesystems())

    def destroy(self):
        shutil.rmtree(self.root)

    def rollback_to(self, snapshot):
        shutil.rmtree(self.mountpoint)
        shutil.copytree(snapshot.data, self.mountpoint)

        # restore any local properties
        if os.path.exists(snapshot.properties):
            shutil.rmtree(self.properties)
            shutil.copytree(snapshot.properties, self.properties)

    def rename(self, new_dataset):
        # re-create relative symlink into pool data
        target = os.path.join('..', '..', 'data', new_dataset.poolless_name)
        try:
            os.makedirs(os.path.join(new_dataset.root, target))
        except OSError:
            # already exists
            pass

        # move each component individually
        os.symlink(target, new_dataset.data)

        # shutil.move treats destination as parent if it is a directory
        #logger.debug(
        #    '%s: %s -> %s', self, self.mountpoint, new_dataset.mountpoint)
        os.rmdir(new_dataset.mountpoint)
        shutil.move(self.mountpoint, new_dataset.mountpoint)
        shutil.move(self.properties, new_dataset.root)
        shutil.move(self.snapshots, new_dataset.root)

        # all data has been moved
        self.destroy()

    def get_snapshots(self):
        return [Snapshot(self.name, x) for x in os.listdir(self.snapshots)]


class Snapshot(Dataset):
    def __init__(self, filesystem, snapshot):
        self.filesystem = Filesystem(filesystem)
        self.name = snapshot
        self.full_name = '%s@%s' % (filesystem, snapshot)
        self.root = os.path.join(self.filesystem.root, 'snapshots', self.name)
        self.pool = self.filesystem.pool

    @property
    def base_attrs(self):
        data = super(Snapshot, self).base_attrs
        data['name'] = self.full_name
        return data

    def exists(self):
        return os.path.exists(self.root)

    def create(self):
        os.makedirs(self.root)
        shutil.copytree(self.filesystem.data, self.data)
        if os.path.exists(self.filesystem.properties):
            shutil.copytree(self.filesystem.properties, self.properties)
        else:
            # no local properties associated with current working filesystem;
            #  use an empty directory for the snapshot's filesystem
            os.makedirs(self.properties)

    def rename(self, new_snapshot):
        os.rename(self.root, new_snapshot.root)

    def clone_to(self, new_filesystem):
        new_filesystem.create()
        #logger.debug('%s: cloning to %s', self, new_filesystem.mountpoint)

        # remove folders to be replaced by copytree
        #logger.debug(
        #    '%s: %s -> %s', self, self.data, new_filesystem.mountpoint)
        os.rmdir(new_filesystem.mountpoint)
        os.rmdir(new_filesystem.properties)
        shutil.copytree(self.data, new_filesystem.mountpoint)
        shutil.copytree(self.properties, new_filesystem.properties)

    def to_stream(self, stream):
        # write a gzipped tar of the snapshot to the stream
        with gzip.GzipFile(fileobj=stream, mode='w') as g:
            with tarfile.open(fileobj=g, mode='w') as t:
                t.add(self.root, arcname=self.name)
