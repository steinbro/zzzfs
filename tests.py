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

import io
import os
import uuid
import shutil
import random
import tempfile
import unittest
import multiprocessing

from libzzzfs import zfs
from libzzzfs.dataset import get_dataset_by
from libzzzfs.util import ZzzFSException
from libzzzfs.cmd.zzzfs import zzzfs_main
from libzzzfs.cmd.zzzpool import zzzpool_main


def zzzcmd(cmdline):
    '''Given a command line string, call zzzfs_main/zzzpool_main, to exercise
    argparse code.
    '''
    args = cmdline.split(' ')
    return globals()[args[0] + '_main'](args)


class ZzzFSTestBase(unittest.TestCase):
    '''Base class for tests. Contains common setUp/tearDown and helper methods.
    '''
    def setUp(self):
        self.zzzfs_root = tempfile.mkdtemp()
        os.environ['ZZZFS_ROOT'] = self.zzzfs_root

        self.zroot1 = tempfile.mkdtemp()
        self.zroot2 = tempfile.mkdtemp()
        zzzcmd('zzzpool create foo ' + self.zroot1)
        zzzcmd('zzzpool create bar ' + self.zroot2)

    def tearDown(self):
        zzzcmd('zzzpool destroy foo')
        zzzcmd('zzzpool destroy bar')
        shutil.rmtree(self.zroot1)
        shutil.rmtree(self.zroot2)
        shutil.rmtree(self.zzzfs_root)

    # helper functions
    def all_files_in(self, directory):
        '''Returns all descendant files of directory with relative paths.'''
        return sorted(os.path.join(dirpath[len(directory)+1:], f)
                for dirpath, dirnames, filenames in os.walk(directory)
                for f in filenames)

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
            zzzcmd('zzzpool create foo ' + self.zroot1)

        zzzcmd('zzzpool destroy foo')

        # destroy twice
        with self.assertRaises(ZzzFSException):
            zzzcmd('zzzpool destroy foo')

        # re-create, so tearDown won't complain
        zzzcmd('zzzpool create foo ' + self.zroot1)

    def test_zpool_history(self):
        self.assertIn('zzzpool create foo', zzzcmd('zzzpool history foo'))

        # zzzfs commands should also show up in zzzpool history
        zzzcmd('zzzfs create foo/subfs')
        self.assertIn(
            'zzzfs create foo/subfs', zzzcmd('zzzpool history foo'))

        # old entries should still be present
        self.assertIn('zzzpool create foo', zzzcmd('zzzpool history foo'))

        # inconsequential zzzfs commands shouldn't show up
        zzzcmd('zzzfs get all foo')
        self.assertNotIn('zzzfs list', zzzcmd('zzzpool history foo'))

        # signle command affecting multiple pools is logged once in each pool
        zzzcmd('zzzfs snapshot foo@first bar@first bar@second')
        self.assertIn('zzzfs snapshot', zzzcmd('zzzpool history foo'))
        self.assertEqual(
            zzzcmd('zzzpool history foo').count('zzzfs snapshot'), 1)

    def test_zpool_list(self):
        self.assertIn('foo', zzzcmd('zzzpool list -H'))
        self.assertIn('bar', zzzcmd('zzzpool list -H'))

        # custom headers, lacking name
        self.assertIn('ONLINE', zzzcmd('zzzpool list -o health'))
        self.assertNotIn('foo', zzzcmd('zzzpool list -o health'))

        # list specific pool
        self.assertIn('foo', zzzcmd('zzzpool list -H foo'))
        self.assertNotIn('bar', zzzcmd('zzzpool list -H foo'))

        # list with non-existent pool
        with self.assertRaises(ZzzFSException):
            zzzcmd('zzzpool list baz')


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
            zzzcmd('zzzfs create foo/missing/subfoo')

        # -p should create missing filesystems
        zzzcmd('zzzfs create -p foo/missing/subfoo')
        self.assertTrue(
            get_dataset_by('foo/missing', should_exist=None).exists())
        self.assertTrue(
            get_dataset_by('foo/missing/subfoo', should_exist=None).exists())

    def test_zfs_create_with_properties(self):
        zzzcmd('zzzfs create -o x=1 -o y=2 foo/subfoo')
        self.assertEqual('1', zzzcmd('zzzfs get -H -o value x foo/subfoo'))
        self.assertEqual('2', zzzcmd('zzzfs get -H -o value y foo/subfoo'))

    def test_zfs_destroy(self):
        # directory in pool data should be removed
        zzzcmd('zzzfs create -p foo/subfoo')
        zzzcmd('zzzfs destroy foo/subfoo')
        self.assertFalse(
            os.path.exists(os.path.join(self.zroot1, 'foo', 'subfoo')))

    def test_zfs_destory_recursive(self):
        # destroy should only remove any child filesystems if -r is specified
        zzzcmd('zzzfs create -p foo/la/dee/da/subfoo')
        with self.assertRaises(ZzzFSException):
            zzzcmd('zzzfs destroy foo/la/dee/da')
        self.assertTrue(
            get_dataset_by('foo/la/dee/da', should_exist=None).exists())

        zzzcmd('zzzfs destroy -r foo/la/dee/da')
        self.assertFalse(
            get_dataset_by('foo/la/dee/da/subfoo', should_exist=None).exists())
        self.assertTrue(
            get_dataset_by('foo/la/dee', should_exist=None).exists())

        # don't mistake matching prefix as parent filesystem
        zzzcmd('zzzfs create foo/la/deeee')
        zzzcmd('zzzfs destroy foo/la/dee')  # should not require -r

    def test_zfs_get_set(self):
        # local vs.inherited properties
        zzzcmd('zzzfs create foo/subfoo')
        zzzcmd('zzzfs set myvar=nothing foo')
        self.assertEqual(
            'nothing',
            zzzcmd('zzzfs get -H -o value -s inherited myvar foo/subfoo'))
        self.assertEqual(
            'nothing', zzzcmd('zzzfs get -H -o value -s local myvar foo'))
        # foo/subfoo should have no local value for myvar
        self.assertEqual(
            '', zzzcmd('zzzfs get -H -s local myvar foo/subfoo'))
        # should appear in "get all", when source matches
        self.assertIn(
            'nothing', zzzcmd('zzzfs get -H -o value -s local all foo'))
        self.assertNotIn(
            'nothing',
            zzzcmd('zzzfs get -H -o value -s inherited all foo'))

        # both filesystems have the same creation time (to the nearest minute)
        self.assertEqual(
            zzzcmd('zzzfs get -H -o value creation foo'),
            zzzcmd('zzzfs get -H -o value creation foo/subfoo'))

        # invalid headers/property names
        with self.assertRaises(ZzzFSException):
            zzzcmd('zzzfs get -o no,such,headers myvar foo')
        with self.assertRaises(ZzzFSException):
            zzzcmd('zzzfs set bad/var/name=something foo')

    def test_zfs_inherit(self):
        zzzcmd('zzzfs create foo/subfoo')
        zzzcmd('zzzfs set myvar=nothing foo')
        zzzcmd('zzzfs set myvar=something foo/subfoo')
        self.assertEqual(
            'nothing', zzzcmd('zzzfs get -H -o value myvar foo'))
        self.assertEqual(
            'something', zzzcmd('zzzfs get -H -o value myvar foo/subfoo'))

        zzzcmd('zzzfs inherit myvar foo/subfoo')
        self.assertEqual(
            'nothing', zzzcmd('zzzfs get -H -o value myvar foo/subfoo'))
        self.assertEqual(
            'inherited', zzzcmd('zzzfs get -H -o source myvar foo/subfoo'))

    def test_zfs_list(self):
        # creation of zpool implicitly creates default ZFS lsit
        self.assertIn('foo', zzzcmd('zzzfs list -H -o name'))
        self.assertIn('bar', zzzcmd('zzzfs list -H -o name'))

        zzzcmd('zzzfs create foo/subfoo')
        self.assertIn('foo/subfoo', zzzcmd('zzzfs list -H -o name'))

        self.assertIn(os.path.join(self.zroot1, 'foo', 'subfoo'), zzzcmd(
            'zzzfs list -H -o mountpoint'))

        zzzcmd('zzzfs create foo/subfoo/subsubfoo')
        self.assertIn(
            'foo/subfoo/subsubfoo', zzzcmd('zzzfs list -H -o name'))

        with self.assertRaises(ZzzFSException):
            zzzcmd('zzzfs list -t no,such,types')

        # custom field names shouldn't throw an exception
        zzzcmd('zzzfs list -o no,such,headers')

    def test_zfs_list_descendants(self):
        zzzcmd('zzzfs create -p foo/la/dee/da/subfoo')

        # without -r or -d, show only the dataset itself
        self.assertEqual(
            'foo/la/dee', zzzcmd('zzzfs list -H -o name foo/la/dee'))

        # or multiple, if so specified
        self.assertSetEqual(
            set(['foo/la', 'foo/la/dee']), set(zzzcmd(
                'zzzfs list -H -o name foo/la foo/la/dee').split('\n')))

        # -r shows all descendants
        self.assertSetEqual(
            set(['foo/la/dee', 'foo/la/dee/da', 'foo/la/dee/da/subfoo']),
            set(zzzcmd('zzzfs list -H -o name -r foo/la/dee').split('\n')))

        # -d shows a specific number of generations
        self.assertSetEqual(
            set(['foo/la/dee', 'foo/la/dee/da']), set(zzzcmd(
                'zzzfs list -H -o name -d 1 foo/la/dee').split('\n')))

        # snapshots of descendants should also be shown
        zzzcmd('zzzfs snapshot foo/la/dee/da@first')
        self.assertEqual(
            'foo/la/dee/da@first',
            zzzcmd('zzzfs list -H -t snap -o name -r foo/la/dee'))

    def test_zfs_list_sort(self):
        # not using assertSetEquals here because order matters, obviously
        self.assertEqual(
            ['bar', 'foo'],
            zzzcmd('zzzfs list -H -o name -s name').split('\n'))

        # descending sort
        self.assertEqual(
            ['foo', 'bar'],
            zzzcmd('zzzfs list -H -o name -S name').split('\n'))

        # sort by any field
        zzzcmd('zzzfs set myprop=1 foo')
        zzzcmd('zzzfs set myprop=2 bar')
        self.assertEqual(
            ['1', '2'],
            zzzcmd('zzzfs list -H -o myprop -s myprop').split('\n'))

        # multiple sort columns: applied left to right
        self.assertEqual(
            'bar\t2\nfoo\t1',
            zzzcmd('zzzfs list -H -o name,myprop -s myprop -s name'))

        # can't sort by a field not shown
        with self.assertRaises(ZzzFSException):
            zzzcmd('zzzfs list -H -o name -s myprop')

    def test_zfs_snapshot(self):
        self.populate_randomly(os.path.join(self.zroot1, 'foo'))
        zzzcmd('zzzfs snapshot foo@first')
        self.assertIn('foo@first', zzzcmd('zzzfs list -t snapshot'))

        # multiple snapshots can be specified in the same command
        zzzcmd('zzzfs snapshot foo@second foo@third')
        self.assertIn('foo@second', zzzcmd('zzzfs list -t snapshot'))
        self.assertIn('foo@third', zzzcmd('zzzfs list -t snapshot'))

        # duplicate snapshot names should fail cleanly
        with self.assertRaises(ZzzFSException):
            zzzcmd('zzzfs snapshot foo@fourth foo@fourth')
        # should have been created once, anyway
        self.assertIn('foo@fourth', zzzcmd('zzzfs list -t snapshot'))

    def test_zfs_snapshot_with_properties(self):
        zzzcmd('zzzfs snapshot -o x=1 -o y=2 foo@first')
        self.assertEqual(
            '1', zzzcmd('zzzfs get -H -t snap -o value x foo@first'))
        self.assertEqual(
            '2', zzzcmd('zzzfs get -H -t snap -o value y foo@first'))

    def test_zfs_rollback(self):
        # both files and properties should be restored
        foo_path = os.path.join(self.zroot1, 'foo')
        self.populate_randomly(foo_path)
        zzzcmd('zzzfs set myvar=nothing foo')

        zzzcmd('zzzfs snapshot foo@first')
        contents_before = self.all_files_in(foo_path)

        # change a property, remove a file
        zzzcmd('zzzfs set myvar=something foo')
        self.delete_something_in(foo_path)
        self.assertNotEqual(self.all_files_in(foo_path), contents_before)
        self.assertEqual(
            'something', zzzcmd('zzzfs get -H -o value myvar foo'))

        zzzcmd('zzzfs rollback foo@first')

        self.assertEqual(self.all_files_in(foo_path), contents_before)
        self.assertEqual(
            'nothing', zzzcmd('zzzfs get -H -o value myvar foo'))

    def test_zfs_send_receive(self):
        zzzcmd('zzzfs create foo/origin')
        foo_path = os.path.join(self.zroot1, 'foo', 'origin')
        self.populate_randomly(foo_path)
        zzzcmd('zzzfs snapshot foo/origin@first')

        # use file-like object to simulate pipe
        buf = io.BytesIO()
        zfs.send('foo/origin@first', stream=buf)
        buf.seek(0)
        zfs.receive('foo/received', stream=buf)

        self.assertIn('foo/received', zzzcmd('zzzfs list -H'))
        self.assertEqual(
            self.all_files_in(os.path.join(self.zroot1, 'foo', 'origin')),
            self.all_files_in(os.path.join(self.zroot1, 'foo', 'received')))

        # receiving a bad stream
        with self.assertRaises(ZzzFSException):
            zfs.receive('foo/newer', stream=io.StringIO(u'not snapshot'))

        # if receive failed, filesystem should not have been created
        self.assertNotIn('foo/newer', zzzcmd('zzzfs list'))

    def test_zfs_diff(self):
        foo_path = os.path.join(self.zroot1, 'foo')
        self.populate_randomly(foo_path)
        zzzcmd('zzzfs snapshot foo@first')

        self.delete_something_in(foo_path)
        self.assertTrue(
            zzzcmd('zzzfs diff foo@first foo').startswith('-\t'))

        # if only one argument, diff against current
        self.assertEqual(
            zzzcmd('zzzfs diff foo@first'),
            zzzcmd('zzzfs diff foo@first foo'))

    def test_zfs_rename_filesystem(self):
        something_path = os.path.join(self.zroot1, 'foo', 'something')
        subfoo_path = os.path.join(self.zroot1, 'foo', 'subfoo')
        zzzcmd('zzzfs create foo/something')
        zzzcmd('zzzfs set myvar=something foo/something')
        self.populate_randomly(something_path)
        contents_before = self.all_files_in(something_path)

        zzzcmd('zzzfs rename foo/something foo/subfoo')

        self.assertTrue(os.path.exists(subfoo_path))
        self.assertFalse(os.path.exists(something_path))
        # properties should have been preserved
        self.assertEqual(
            'something', zzzcmd('zzzfs get -H -o value myvar foo/subfoo'))
        # files should have been preserved
        self.assertEqual(contents_before, self.all_files_in(subfoo_path))

    def test_zfs_rename_snapshot(self):
        zzzcmd('zzzfs snapshot foo@first')
        zzzcmd('zzzfs rename foo@first foo@second')

        self.assertNotIn('foo@first', zzzcmd('zzzfs list -t snapshot'))
        self.assertIn('foo@second', zzzcmd('zzzfs list -t snapshot'))

        zzzcmd('zzzfs snapshot foo@third')
        zzzcmd('zzzfs rename foo@third fourth')

        self.assertNotIn('foo@third', zzzcmd('zzzfs list -t snapshot'))
        self.assertIn('foo@fourth', zzzcmd('zzzfs list -t snapshot'))

        # try to rename to different parent filesystem
        zzzcmd('zzzfs create foo/subfoo')
        zzzcmd('zzzfs snapshot foo/subfoo@first')
        with self.assertRaises(ZzzFSException):
            zzzcmd('zzzfs rename foo/subfoo@first foo@first')

    def test_zfs_clone_promote(self):
        # sample use case from FreeBSD man zfs(8), example #10
        production_path = os.path.join(self.zroot1, 'foo', 'production')
        beta_path = os.path.join(self.zroot1, 'foo', 'beta')

        zzzcmd('zzzfs create foo/production')
        self.populate_randomly(production_path)
        zzzcmd('zzzfs snapshot foo/production@today')

        zzzcmd('zzzfs clone foo/production@today foo/beta')
        self.assertEqual(
            self.all_files_in(beta_path), self.all_files_in(production_path))
        self.assertEqual(
            'foo/production@today',
            zzzcmd('zzzfs get -H -o value origin foo/beta'))

        self.delete_something_in(beta_path)
        beta_contents = self.all_files_in(beta_path)
        zzzcmd('zzzfs promote foo/beta')
        self.assertEqual('', zzzcmd('zzzfs get -H -o value origin foo/beta'))

        zzzcmd('zzzfs rename foo/production foo/legacy')
        zzzcmd('zzzfs rename foo/beta foo/production')
        zzzcmd('zzzfs destroy foo/legacy')

        self.assertEqual(self.all_files_in(production_path), beta_contents)
        self.assertNotIn('foo/beta', zzzcmd('zzzfs list'))


class ConcurrencyTest(unittest.TestCase):
    '''Test thread safety of filesystem create/destroy.'''
    THREAD_COUNT = 10

    def setUp(self):
        self.zzzfs_root = tempfile.mkdtemp()
        os.environ['ZZZFS_ROOT'] = self.zzzfs_root

    def tearDown(self):
        shutil.rmtree(self.zzzfs_root)

    def test_list_thread_safety(self):
        class Writer(multiprocessing.Process):
            '''Creates and immediately destroys a zzzpool.'''
            def run(self):
                try:
                    # random unique name, in a random unique folder
                    pool_name = 'zzzpool_test_%s' % uuid.uuid4().hex[:6]
                    zroot = tempfile.mkdtemp()

                    zzzcmd('zzzpool create %s %s' % (pool_name, zroot))
                    zzzcmd('zzzpool destroy %s' % pool_name)

                finally:
                    shutil.rmtree(zroot)

        class Reader(multiprocessing.Process):
            '''Tries to list all datasets.'''
            def run(self):
                zzzcmd('zzzfs list')

        workers = []
        for _ in range(self.THREAD_COUNT):
            # interleave readers/writers
            for some_worker in (Writer, Reader):
                workers.append(some_worker())
                workers[-1].start()

        # wait for all threads to finish
        for w in workers:
            w.join()

        # everything should have exited cleanly
        self.assertTrue(all(w.exitcode == 0 for w in workers))


if __name__ == '__main__':
    unittest.main()
