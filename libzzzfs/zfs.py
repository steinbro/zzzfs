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
import sys
import shutil
import filecmp

from dataset import get_dataset_by, Filesystem, Pool, Snapshot
from util import (
    PropertyList, tabulated, validate_component_name, ZzzFSException)


def clone(snapshot, filesystem):
    '''Turn a snapshot into a filesystem with a new name.'''
    dataset1 = get_dataset_by(snapshot, should_be=Snapshot)
    dataset2 = get_dataset_by(
        filesystem, should_be=Filesystem, should_exist=False)

    dataset1.clone_to(dataset2)


def create(filesystem):
    '''Create a filesystem.'''
    dataset = get_dataset_by(
        filesystem, should_be=Filesystem, should_exist=False)
    # pool should exist, even though dataset itself shouldn't
    if not dataset.pool.exists():
        raise ZzzFSException, '%s: no such pool' % dataset.pool.name
    dataset.create()


def destroy(filesystem):
    '''Remove a filesystem.'''
    get_dataset_by(filesystem, should_be=Filesystem).destroy()


def diff(identifier, other_identifier):
    '''Diff a snapshot against another snapshot in the same filesystem, or
    against the current working filesystem.
    '''
    dataset1 = get_dataset_by(identifier, should_be=Snapshot)
    dataset2 = get_dataset_by(other_identifier)
    # real ZFS can't diff snapshots in different filesystem; not so in ZzzFS
    #if isinstance(dataset2, Filesystem) and (
    #        dataset1.filesystem.name != dataset2.filesystem.name):
    #    raise ZzzFSException, (
    #        '%s: cannot compare to a different filesystem' % identifier)

    output = []
    def do_diff(dcmp):
        # trim off pool root from diff output
        base_path = dcmp.left[len(dataset1.data)+1:]
        for name in dcmp.diff_files:
            output.append('M\t%s' % os.path.join(base_path, name))
        for name in dcmp.left_only:
            output.append('-\t%s' % os.path.join(base_path, name))
        for name in dcmp.right_only:
            output.append('+\t%s' % os.path.join(base_path, name))
        for sub_dcmp in dcmp.subdirs.values():
            do_diff(sub_dcmp)
    do_diff(filecmp.dircmp(dataset1.data, dataset2.data))

    return '\n'.join(output)


def get(properties=PropertyList('all'), identifiers=[],
        headers=PropertyList('all'), scriptable_mode=False):
    '''Get a set of properties for a set of datasets.'''
    if headers.items == ['all']:
        headers = PropertyList('name,property,value,source')
    else:
        headers.validate_against(['name', 'property', 'value', 'source'])

    datasets = [get_dataset_by(identifier) for identifier in identifiers]
    attrs = []
    for dataset in datasets:
        if properties.items == ['all']:
            for key, val in dataset.get_local_properties().iteritems():
                attrs.append({
                    'name': dataset.name, 'property': key, 'value': val,
                    'source': 'local'})

            for key, val in dataset.get_inherited_properties().iteritems():
                attrs.append({
                    'name': dataset.name, 'property': key, 'value': val,
                    'source': 'inherited'})

        else:
            for p in properties.items:
                val, source = dataset.get_property_and_source(p)
                attrs.append({
                    'name': dataset.name, 'property': p, 'value': val,
                    'source': source})

    return tabulated(attrs, headers, scriptable_mode)


def inherit(property, identifiers=[]):
    '''Remove a local property from a set of datasets.'''
    if not validate_component_name(property):
        raise ZzzFSException, '%s: invalid property' % property

    datasets = [get_dataset_by(identifier) for identifier in identifiers]
    for dataset in datasets:
        try:
            os.remove(os.path.join(dataset.properties, property))
        except OSError:
            # property was not set locally
            pass


def list(types=PropertyList('filesystems'), scriptable_mode=False,
         headers=PropertyList('name,used,available,refer,mountpoint')):
    '''Tabulate a set of properties for a set of datasets.'''
    types.validate_against(['all', 'filesystems', 'snapshots', 'snap'])

    filesystems = [f for p in Pool.all() for f in p.get_filesystems()]
    datasets = []
    records = []
    if any(t in ('all', 'filesystems') for t in types.items):
        datasets = filesystems

    if any(t in ('all', 'snapshots', 'snap') for t in types.items):
        datasets += [s for f in filesystems for s in f.get_snapshots()]

    for d in datasets:
        records.append(dict((h, d.get_property(h)) for h in headers.names))

    return tabulated(records, headers, scriptable_mode)


def promote(clone_filesystem):
    '''Turn a cloned snapshot into a standalone filesystem.'''
    # Since there are no actual dependencies in ZzzFS, this is a no-op.
    get_dataset_by(clone_filesystem, should_be=Filesystem, should_exist=True)


def receive(filesystem, stream=sys.stdin):
    '''Create a new filesystem pre-populated with the contens of a snapshot
    sent via zzzfs send piped through stdin.
    '''
    dataset = get_dataset_by(
        filesystem, should_be=Filesystem, should_exist=False)
    # pool should exist, even though dataset itself shouldn't
    if not dataset.pool.exists():
        raise ZzzFSException, '%s: no such pool' % dataset.pool.name
    dataset.create(from_stream=stream)


def rename(identifier, other_identifier):
    '''Move or rename the dataset.'''
    dataset1 = get_dataset_by(identifier)
    dataset2 = None  # may be filesystem or snapshot, will check below

    if isinstance(dataset1, Snapshot):
        dataset2 = get_dataset_by(other_identifier, should_exist=None)
        if isinstance(dataset2, Filesystem):
            # second argument might be snapshot alone, which we'd interpret as
            # a filesystem; e.g. "rename fs@snapshot new_snapshot"
            other_identifier = '%s@%s' % (
                dataset1.filesystem.name, other_identifier)

        # re-identify with should_exist
        dataset2 = get_dataset_by(
            other_identifier, should_be=Snapshot, should_exist=False)

        # both snapshots
        if dataset1.filesystem.name != dataset2.filesystem.name:
            raise ZzzFSException, 'mismatched filesystems'

    else:  # dataset1 is a filesystem
        dataset2 = get_dataset_by(
            other_identifier, should_be=Filesystem, should_exist=False)

        if dataset1.pool.name != dataset2.pool.name:
            raise ZzzFSException, 'cannot rename to different pool'

    # same procedure whether filesystem or snapshot
    dataset1.rename(dataset2)


def rollback(snapshot):
    '''Replace the filesystem with the contents of the spceified snapshot.'''
    dataset = get_dataset_by(snapshot, should_be=Snapshot)
    dataset.filesystem.rollback_to(dataset)


def send(snapshot, stream=sys.stdout):
    '''Create a gzipped tarball of a snapshot and write it to sdout.'''
    get_dataset_by(snapshot, should_be=Snapshot).to_stream(stream)


def set(keyval, identifiers):
    '''Set a property value for a set of datasets.'''
    try:
        key, val = keyval.split('=')
    except ValueError:
        raise ZzzFSException, '%r: invalid property=value format' % keyval

    if not validate_component_name(key):
        raise ZzzFSException, '%s: invalid property' % key

    datasets = [get_dataset_by(identifier) for identifier in identifiers]
    for dataset in datasets:
        dataset.add_local_property(key, val)


def snapshot(snapshots):
    '''Create a snapshot of a filesystem.'''
    datasets = [get_dataset_by(
        i, should_be=Snapshot, should_exist=False) for i in snapshots]
    for dataset in datasets:
        if not dataset.filesystem.exists():
            raise ZzzFSException, (
                '%s: no such filesystem' % dataset.filesystem.name)
        dataset.create()
