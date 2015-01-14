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

from libzzzfs import zfs
from libzzzfs.dataset import get_dataset_by
from libzzzfs.util import ZzzFSException
from zzzfs import zzzfs_main
from zzzpool import zzzpool_main


class ZzzFSTestBase(unittest.TestCase):
    '''Base class for tests. Contains common setUp/tearDown and helper methods.
    '''
    def setUp(self):
        self.zzzfs_root = tempfile.mkdtemp()
        os.environ['ZZZFS_ROOT'] = self.zzzfs_root

        self.zroot1 = tempfile.mkdtemp()
        self.zroot2 = tempfile.mkdtemp()
        self.zzzcmd('zzzpool create foo ' + self.zroot1)
        self.zzzcmd('zzzpool create bar ' + self.zroot2)

    def tearDown(self):
        self.zzzcmd('zzzpool destroy foo')
        self.zzzcmd('zzzpool destroy bar')
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

    def zzzcmd(self, cmdline):
        '''Given a command line string, call zzzfs_main/zzzpool_main, to
        exercise argparse code.
        '''
        args = cmdline.split(' ')
        return globals()[args[0] + '_main'](args)


class ZPoolTest(ZzzFSTestBase):
    def test_zpool_create_destroy(self):
        # create when already exists
        with self.assertRaises(ZzzFSException):
            self.zzzcmd('zzzpool create foo ' + self.zroot1)

        self.zzzcmd('zzzpool destroy foo')

        # destroy twice
        with self.assertRaises(ZzzFSException):
            self.zzzcmd('zzzpool destroy foo')

        # re-create, so tearDown won't complain
        self.zzzcmd('zzzpool create foo ' + self.zroot1)

    def test_zpool_history(self):
        self.assertIn('zzzpool create foo', self.zzzcmd('zzzpool history foo'))

        # zzzfs commands should also show up in zzzpool history
        self.zzzcmd('zzzfs create foo/subfs')
        self.assertIn(
            'zzzfs create foo/subfs', self.zzzcmd('zzzpool history foo'))

        # old entries should still be present
        self.assertIn('zzzpool create foo', self.zzzcmd('zzzpool history foo'))

        # inconsequential zzzfs commands shouldn't show up
        self.zzzcmd('zzzfs get all foo')
        self.assertNotIn('zzzfs list', self.zzzcmd('zzzpool history foo'))

        # signle command affecting multiple pools is logged once in each pool
        self.zzzcmd('zzzfs snapshot foo@first bar@first bar@second')
        self.assertIn('zzzfs snapshot', self.zzzcmd('zzzpool history foo'))
        self.assertEqual(
            self.zzzcmd('zzzpool history foo').count('zzzfs snapshot'), 1)

    def test_zpool_list(self):
        self.assertIn('foo', self.zzzcmd('zzzpool list -H'))
        self.assertIn('bar', self.zzzcmd('zzzpool list -H'))

        # custom headers, lacking name
        self.assertIn('ONLINE', self.zzzcmd('zzzpool list -o health'))
        self.assertNotIn('foo', self.zzzcmd('zzzpool list -o health'))

        # list specific pool
        self.assertIn('foo', self.zzzcmd('zzzpool list -H foo'))
        self.assertNotIn('bar', self.zzzcmd('zzzpool list -H foo'))

        # list with non-existent pool
        with self.assertRaises(ZzzFSException):
            self.zzzcmd('zzzpool list baz')


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

    def test_zfs_create(self):
        # missing intermediate filesystems
        with self.assertRaises(ZzzFSException):
            self.zzzcmd('zzzfs create foo/missing/subfoo')

        # -p should create missing filesystems
        self.zzzcmd('zzzfs create -p foo/missing/subfoo')
        self.assertTrue(
            get_dataset_by('foo/missing', should_exist=None).exists())
        self.assertTrue(
            get_dataset_by('foo/missing/subfoo', should_exist=None).exists())

    def test_zfs_get_set(self):
        # local vs.inherited properties
        self.zzzcmd('zzzfs create foo/subfoo')
        self.zzzcmd('zzzfs set myvar=nothing foo')
        self.assertEqual(
            'nothing', self.zzzcmd('zzzfs get -H -o value myvar foo/subfoo'))
        self.assertEqual(
            'inherited', self.zzzcmd('zzzfs get -H -o source myvar foo/subfoo'))

        self.assertEqual(
            'nothing', self.zzzcmd('zzzfs get -H -o value myvar foo'))
        self.assertEqual(
            'local', self.zzzcmd('zzzfs get -H -o source myvar foo'))

        with self.assertRaises(ZzzFSException):
            self.zzzcmd('zzzfs get -o no,such,headers myvar foo')

        with self.assertRaises(ZzzFSException):
            self.zzzcmd('zzzfs set bad/var/name=something foo')

    def test_zfs_inherit(self):
        self.zzzcmd('zzzfs create foo/subfoo')
        self.zzzcmd('zzzfs set myvar=nothing foo')
        self.zzzcmd('zzzfs set myvar=something foo/subfoo')
        self.assertEqual(
            'nothing', self.zzzcmd('zzzfs get -H -o value myvar foo'))
        self.assertEqual(
            'something', self.zzzcmd('zzzfs get -H -o value myvar foo/subfoo'))

        self.zzzcmd('zzzfs inherit myvar foo/subfoo')
        self.assertEqual(
            'nothing', self.zzzcmd('zzzfs get -H -o value myvar foo/subfoo'))
        self.assertEqual(
            'inherited', self.zzzcmd('zzzfs get -H -o source myvar foo/subfoo'))

    def test_zfs_list(self):
        # creation of zpool implicitly creates default ZFS lsit
        self.assertIn('foo', self.zzzcmd('zzzfs list -H -o name'))
        self.assertIn('bar', self.zzzcmd('zzzfs list -H -o name'))

        self.zzzcmd('zzzfs create foo/subfoo')
        self.assertIn('foo/subfoo', self.zzzcmd('zzzfs list -H -o name'))

        self.assertIn(os.path.join(self.zroot1, 'foo', 'subfoo'), self.zzzcmd(
            'zzzfs list -H -o mountpoint'))

        self.zzzcmd('zzzfs create foo/subfoo/subsubfoo')
        self.assertIn(
            'foo/subfoo/subsubfoo', self.zzzcmd('zzzfs list -H -o name'))

        with self.assertRaises(ZzzFSException):
            self.zzzcmd('zzzfs list -t no,such,types')

        # custom field names shouldn't throw an exception
        self.zzzcmd('zzzfs list -o no,such,headers')

    def test_zfs_snapshot(self):
        self.populate_randomly(os.path.join(self.zroot1, 'foo'))
        self.zzzcmd('zzzfs snapshot foo@first')
        self.assertIn('foo@first', self.zzzcmd('zzzfs list -t snapshots'))

        # multiple snapshots can be specified in the same command
        self.zzzcmd('zzzfs snapshot foo@second foo@third')
        self.assertIn('foo@second', self.zzzcmd('zzzfs list -t snapshots'))
        self.assertIn('foo@third', self.zzzcmd('zzzfs list -t snapshots'))

        # duplicate snapshot names should fail cleanly
        with self.assertRaises(ZzzFSException):
            self.zzzcmd('zzzfs snapshot foo@fourth foo@fourth')
        # should have been created once, anyway
        self.assertIn('foo@fourth', self.zzzcmd('zzzfs list -t snapshots'))

    def test_zfs_rollback(self):
        # both files and properties should be restored
        foo_path = os.path.join(self.zroot1, 'foo')
        self.populate_randomly(foo_path)
        self.zzzcmd('zzzfs set myvar=nothing foo')

        self.zzzcmd('zzzfs snapshot foo@first')
        contents_before = self.all_files_in(foo_path)

        # change a property, remove a file
        self.zzzcmd('zzzfs set myvar=something foo')
        self.delete_something_in(foo_path)
        self.assertNotEqual(self.all_files_in(foo_path), contents_before)
        self.assertEqual(
            'something', self.zzzcmd('zzzfs get -H -o value myvar foo'))

        self.zzzcmd('zzzfs rollback foo@first')

        self.assertEqual(self.all_files_in(foo_path), contents_before)
        self.assertEqual(
            'nothing', self.zzzcmd('zzzfs get -H -o value myvar foo'))

    def test_zfs_send_receive(self):
        self.zzzcmd('zzzfs create foo/origin')
        foo_path = os.path.join(self.zroot1, 'foo', 'origin')
        self.populate_randomly(foo_path)
        self.zzzcmd('zzzfs snapshot foo/origin@first')

        # use file-like object to simulate pipe
        buf = cStringIO.StringIO()
        zfs.send('foo/origin@first', stream=buf)
        buf.seek(0)
        zfs.receive('foo/received', stream=buf)

        self.assertIn('foo/received', self.zzzcmd('zzzfs list -H'))
        self.assertEqual(
            self.all_files_in(os.path.join(self.zroot1, 'foo', 'origin')),
            self.all_files_in(os.path.join(self.zroot1, 'foo', 'received')))

        # receiving a bad stream
        with self.assertRaises(ZzzFSException):
            zfs.receive('foo/newer', stream=cStringIO.StringIO('not snapshot'))

        # if receive failed, filesystem should not have been created
        self.assertNotIn('foo/newer', self.zzzcmd('zzzfs list'))

    def test_zfs_diff(self):
        foo_path = os.path.join(self.zroot1, 'foo')
        self.populate_randomly(foo_path)
        self.zzzcmd('zzzfs snapshot foo@first')

        self.delete_something_in(foo_path)
        self.assertTrue(
            self.zzzcmd('zzzfs diff foo@first foo').startswith('-\t'))

    def test_zfs_rename(self):
        something_path = os.path.join(self.zroot1, 'foo', 'something')
        subfoo_path = os.path.join(self.zroot1, 'foo', 'subfoo')
        self.zzzcmd('zzzfs create foo/something')
        self.zzzcmd('zzzfs set myvar=something foo/something')
        self.populate_randomly(something_path)
        contents_before = self.all_files_in(something_path)

        self.zzzcmd('zzzfs rename foo/something foo/subfoo')

        self.assertTrue(os.path.exists(subfoo_path))
        self.assertFalse(os.path.exists(something_path))
        # properties should have been preserved
        self.assertEqual(
            'something', self.zzzcmd('zzzfs get -H -o value myvar foo/subfoo'))
        # files should have been preserved
        self.assertEqual(contents_before, self.all_files_in(subfoo_path))

        self.zzzcmd('zzzfs snapshot foo/subfoo@first')
        self.zzzcmd('zzzfs rename foo/subfoo@first foo/subfoo@second')

        self.assertNotIn(
            'foo/subfoo@first', self.zzzcmd('zzzfs list -t snapshots'))
        self.assertIn(
            'foo/subfoo@second', self.zzzcmd('zzzfs list -t snapshots'))

        self.zzzcmd('zzzfs snapshot foo/subfoo@third')
        self.zzzcmd('zzzfs rename foo/subfoo@third fourth')

        self.assertNotIn(
            'foo/subfoo@third', self.zzzcmd('zzzfs list -t snapshots'))
        self.assertIn(
            'foo/subfoo@fourth', self.zzzcmd('zzzfs list -t snapshots'))

        with self.assertRaises(ZzzFSException):
            self.zzzcmd('zzzfs rename foo/subfoo@first foo@first')

    def test_zfs_clone_promote(self):
        # sample use case from FreeBSD man zfs(8), example #10
        production_path = os.path.join(self.zroot1, 'foo', 'production')
        beta_path = os.path.join(self.zroot1, 'foo', 'beta')

        self.zzzcmd('zzzfs create foo/production')
        self.populate_randomly(production_path)
        self.zzzcmd('zzzfs snapshot foo/production@today')

        self.zzzcmd('zzzfs clone foo/production@today foo/beta')
        self.assertEqual(
            self.all_files_in(beta_path), self.all_files_in(production_path))

        self.delete_something_in(beta_path)
        beta_contents = self.all_files_in(beta_path)
        self.zzzcmd('zzzfs promote foo/beta')
        self.zzzcmd('zzzfs rename foo/production foo/legacy')
        self.zzzcmd('zzzfs rename foo/beta foo/production')
        self.zzzcmd('zzzfs destroy foo/legacy')

        self.assertEqual(self.all_files_in(production_path), beta_contents)
        self.assertNotIn('foo/beta', self.zzzcmd('zzzfs list'))


if __name__ == '__main__':
    unittest.main()