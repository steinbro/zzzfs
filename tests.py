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

import os
import shutil
import random
import tempfile
import unittest
import cStringIO

from libzzzfs import zfs, zpool
from libzzzfs.dataset import get_dataset_by
from libzzzfs.util import PropertyList, ZzzFSException


class ZzzFSTestBase(unittest.TestCase):
    '''Base class for tests. Contains common setUp/tearDown and helper methods.
    '''
    def setUp(self):
        self.zzzfs_root = tempfile.mkdtemp()
        os.environ['ZZZFS_ROOT'] = self.zzzfs_root

        self.zroot1 = tempfile.mkdtemp()
        self.zroot2 = tempfile.mkdtemp()
        zpool.create('foo', self.zroot1)
        zpool.create('bar', self.zroot2)

    def tearDown(self):
        zpool.destroy('foo')
        zpool.destroy('bar')
        shutil.rmtree(self.zroot1)
        shutil.rmtree(self.zroot2)
        shutil.rmtree(self.zzzfs_root)

    # helper functions
    def all_files_in(self, directory):
        '''Returns all descendant files of directory with relative paths.'''
        return [os.path.join(dirpath[len(directory)+1:], f)
                for dirpath, dirnames, filenames in os.walk(directory)
                for f in filenames]

    def populate_randomly(self, directory, max_subdirs=10, max_depth=3):
        '''Creates a set of (randomly) between 1 and max_subdirs subdirectories
        in the specified directory, each having (randomly) between 1 and
        max_depth subdirectories, the leaf node of each containing an empty
        file.
        '''
        for n in range(random.randint(1, max_subdirs)):
            subdir = '/'.join(
                '%d' % n for i in range(random.randint(1, max_depth)))
            path = os.path.join(directory, subdir)
            filename = os.path.join(path, 'somefile')
            os.makedirs(path)
            with open(os.path.join(path, filename), 'a') as f:
                os.utime(path, None)

    def delete_something_in(self, directory):
        '''Deletes a random descendant file in directory's subtree.'''
        os.remove(os.path.join(directory, random.choice(
            self.all_files_in(directory))))


class ZPoolTest(ZzzFSTestBase):
    def test_zpool_create_destroy(self):
        # create when already exists
        with self.assertRaises(ZzzFSException):
            zpool.create('foo', self.zroot1)

        zpool.destroy('foo')

        # destroy twice
        with self.assertRaises(ZzzFSException):
            zpool.destroy('foo')

        # re-crete, so tearDown won't complain
        zpool.create('foo', self.zroot1)

    def test_zpool_list(self):
        output = zpool.list(scriptable_mode=True)
        self.assertIn('foo', output)
        self.assertIn('bar', output)

        # custom headers, lacking name
        output = zpool.list(headers=PropertyList('health'))
        self.assertIn('ONLINE', output)
        self.assertNotIn('foo', output)

        # list specific pool
        output = zpool.list('foo', scriptable_mode=True)
        self.assertIn('foo', output)
        self.assertNotIn('bar', output)

        # list with non-existent pool
        with self.assertRaises(ZzzFSException):
            zpool.list('baz')


class ZFSTest(ZzzFSTestBase):
    def test_bad_dataset_names(self):
        with self.assertRaises(ZzzFSException):
            get_dataset_by('foo@bar@baz', should_exist=False)
        with self.assertRaises(ZzzFSException):
            get_dataset_by('foo@bar/baz', should_exist=False)
        with self.assertRaises(ZzzFSException):
            get_dataset_by('@bar', should_exist=False)
        with self.assertRaises(ZzzFSException):
            get_dataset_by('_foo@bar', should_exist=False)
        with self.assertRaises(ZzzFSException):
            get_dataset_by('foo@bar!', should_exist=False)

    def test_zfs_get_set(self):
        # local vs.inherited properties
        zfs.create('foo/subfoo')
        zfs.set('myvar=nothing', ['foo'])
        self.assertEqual('nothing', zfs.get(
            PropertyList('myvar'), ['foo/subfoo'],
            headers=PropertyList('value'), scriptable_mode=True))
        self.assertEqual('inherited', zfs.get(
            PropertyList('myvar'), ['foo/subfoo'],
            headers=PropertyList('source'), scriptable_mode=True))

        self.assertEqual('nothing', zfs.get(
            PropertyList('myvar'), ['foo'],
            headers=PropertyList('value'), scriptable_mode=True).strip())
        self.assertEqual('local', zfs.get(
            PropertyList('myvar'), ['foo'],
            headers=PropertyList('source'), scriptable_mode=True).strip())

        with self.assertRaises(ZzzFSException):
            zfs.get(PropertyList('myvar'), ['foo'],
                headers=PropertyList('no,such,headers'))

        with self.assertRaises(ZzzFSException):
            zfs.set('bad/var/name=something', ['foo'])

    def test_zfs_inherit(self):
        zfs.create('foo/subfoo')
        zfs.set('myvar=nothing', ['foo'])
        zfs.set('myvar=something', ['foo/subfoo'])
        self.assertEqual('nothing', zfs.get(
            PropertyList('myvar'), ['foo'],
            headers=PropertyList('value'), scriptable_mode=True))
        self.assertEqual('something', zfs.get(
            PropertyList('myvar'), ['foo/subfoo'],
            headers=PropertyList('value'), scriptable_mode=True))

        zfs.inherit('myvar', ['foo/subfoo'])
        self.assertEqual('nothing', zfs.get(
            PropertyList('myvar'), ['foo/subfoo'],
            headers=PropertyList('value'), scriptable_mode=True))
        self.assertEqual('inherited', zfs.get(
            PropertyList('myvar'), ['foo/subfoo'],
            headers=PropertyList('source'), scriptable_mode=True))

    def test_zfs_list(self):
        # creation of zpool implicitly creates default ZFS lsit
        self.assertIn('foo', zfs.list(
            headers=PropertyList('name'), scriptable_mode=True))
        self.assertIn('bar', zfs.list(
            headers=PropertyList('name'), scriptable_mode=True))

        zfs.create('foo/subfoo')
        self.assertIn('foo/subfoo', zfs.list(
            headers=PropertyList('name'), scriptable_mode=True))

        self.assertIn(os.path.join(self.zroot1, 'foo', 'subfoo'), zfs.list(
            headers=PropertyList('mountpoint'), scriptable_mode=True))

        zfs.create('foo/subfoo/subsubfoo')
        self.assertIn('foo/subfoo/subsubfoo', zfs.list(
            headers=PropertyList('name'), scriptable_mode=True))

        with self.assertRaises(ZzzFSException):
            zfs.list(types=PropertyList('no,such,types'))

    def test_zfs_snapshot(self):
        self.populate_randomly(os.path.join(self.zroot1, 'foo'))
        zfs.snapshot(['foo@first'])
        self.assertIn('foo@first', zfs.list(types=PropertyList('snapshots')))

    def test_zfs_rollback(self):
        # both files and properties should be restored
        foo_path = os.path.join(self.zroot1, 'foo')
        self.populate_randomly(foo_path)
        zfs.set('myvar=nothing', ['foo'])

        zfs.snapshot(['foo@first'])
        contents_before = self.all_files_in(foo_path)

        # change a property, remove a file
        zfs.set('myvar=something', ['foo'])
        self.delete_something_in(foo_path)
        self.assertNotEqual(self.all_files_in(foo_path), contents_before)
        self.assertEqual('something', zfs.get(
            PropertyList('myvar'), ['foo'], headers=PropertyList('value'),
            scriptable_mode=True))

        zfs.rollback('foo@first')

        self.assertEqual(self.all_files_in(foo_path), contents_before)
        self.assertEqual('nothing', zfs.get(
            PropertyList('myvar'), ['foo'], headers=PropertyList('value'),
            scriptable_mode=True))

    def test_zfs_send_receive(self):
        zfs.create('foo/origin')
        foo_path = os.path.join(self.zroot1, 'foo', 'origin')
        self.populate_randomly(foo_path)
        zfs.snapshot(['foo/origin@first'])

        # use file-like object to simulate pipe
        buf = cStringIO.StringIO()
        zfs.send('foo/origin@first', stream=buf)
        buf.seek(0)
        zfs.receive('foo/received', stream=buf)

        self.assertIn('foo/received', zfs.list(scriptable_mode=True))
        self.assertEqual(
            self.all_files_in(os.path.join(self.zroot1, 'foo', 'origin')),
            self.all_files_in(os.path.join(self.zroot1, 'foo', 'received')))

        # receiving a bad stream
        with self.assertRaises(ZzzFSException):
            zfs.receive('foo/newer', stream=cStringIO.StringIO('not snapshot'))

        # if receive failed, filesystem should not have been created
        self.assertNotIn('foo/newer', zfs.list())

    def test_zfs_diff(self):
        foo_path = os.path.join(self.zroot1, 'foo')
        self.populate_randomly(foo_path)
        zfs.snapshot(['foo@first'])

        self.delete_something_in(foo_path)
        self.assertTrue(zfs.diff('foo@first', 'foo').startswith('-\t'))

    def test_zfs_rename(self):
        something_path = os.path.join(self.zroot1, 'foo', 'something')
        subfoo_path = os.path.join(self.zroot1, 'foo', 'subfoo')
        zfs.create('foo/something')
        zfs.set('myvar=something', ['foo/something'])
        self.populate_randomly(something_path)
        contents_before = self.all_files_in(something_path)

        zfs.rename('foo/something', 'foo/subfoo')

        self.assertTrue(os.path.exists(subfoo_path))
        self.assertFalse(os.path.exists(something_path))
        # properties should have been preserved
        self.assertEqual('something', zfs.get(
            PropertyList('myvar'), ['foo/subfoo'],
            headers=PropertyList('value'), scriptable_mode=True))
        # files should have been preserved
        self.assertEqual(contents_before, self.all_files_in(subfoo_path))

        zfs.snapshot(['foo/subfoo@first'])
        zfs.rename('foo/subfoo@first', 'foo/subfoo@second')

        self.assertNotIn(
            'foo/subfoo@first', zfs.list(types=PropertyList('snapshots')))
        self.assertIn(
            'foo/subfoo@second', zfs.list(types=PropertyList('snapshots')))

        zfs.snapshot(['foo/subfoo@third'])
        zfs.rename('foo/subfoo@third', 'fourth')

        self.assertNotIn(
            'foo/subfoo@third', zfs.list(types=PropertyList('snapshots')))
        self.assertIn(
            'foo/subfoo@fourth', zfs.list(types=PropertyList('snapshots')))

        with self.assertRaises(ZzzFSException):
            zfs.rename('foo/subfoo@first', 'foo@first')

    def test_zfs_clone_promote(self):
        # sample use case from FreeBSD man zfs(8), example #10
        production_path = os.path.join(self.zroot1, 'foo', 'production')
        beta_path = os.path.join(self.zroot1, 'foo', 'beta')

        zfs.create('foo/production')
        self.populate_randomly(production_path)
        zfs.snapshot(['foo/production@today'])

        zfs.clone('foo/production@today', 'foo/beta')
        self.assertEqual(
            self.all_files_in(beta_path), self.all_files_in(production_path))

        self.delete_something_in(beta_path)
        beta_contents = self.all_files_in(beta_path)
        zfs.promote('foo/beta')
        zfs.rename('foo/production', 'foo/legacy')
        zfs.rename('foo/beta', 'foo/production')
        zfs.destroy('foo/legacy')

        self.assertEqual(self.all_files_in(production_path), beta_contents)
        self.assertNotIn('foo/beta', zfs.list())


if __name__ == '__main__':
    unittest.main()
