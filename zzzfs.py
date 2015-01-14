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

import sys
import argparse
from libzzzfs import zfs
from libzzzfs.dataset import Dataset, Pool
from libzzzfs.util import PropertyList, ZzzFSException


def zzzfs_main(argv):
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='command', title='subcommands')

    # per-command arguments
    clone = subparsers.add_parser(
        'clone', help='turn a snapshot into a filesystem with a new name')
    clone.add_argument('snapshot')
    clone.add_argument('filesystem')

    create = subparsers.add_parser('create', help='create a filesystem')
    create.add_argument('filesystem')

    destroy = subparsers.add_parser('destroy', help='destroy a filesystem')
    destroy.add_argument('filesystem')

    diff = subparsers.add_parser(
        'diff', help='compare filesystem/snapshot against a snapshot')
    diff.add_argument('identifier', metavar='snapshot')
    diff.add_argument('other_identifier', metavar='snapshot|filesystem')

    get = subparsers.add_parser('get', help='get dataset properties')
    get.add_argument(
        'properties', metavar='all | property[,property...]', type=PropertyList,
        help='comma-separated list of properties')
    get.add_argument(
        'identifiers', metavar='filesystem|snapshot', nargs='+')
    get.add_argument(
        '-H', action='store_true', dest='scriptable_mode',
        help='scripted mode (no headers, tab-delimited)')
    get.add_argument(
        '-o', metavar='all | field[,field...]', type=PropertyList,
        default=PropertyList('all'), dest='headers',
        help='comma-separated list of fields (name, property, value, source)')

    inherit = subparsers.add_parser(
        'inherit', help='unset a property from datasets')
    inherit.add_argument('property')
    inherit.add_argument(
        'identifiers', metavar='filesystem|snapshot', nargs='+')

    list_ = subparsers.add_parser('list', help='list datasets')
    list_.add_argument(
        '-t', metavar='type[,type...]', dest='types', type=PropertyList,
        default=PropertyList('filesystems'),
        help='comma-separated list of types (all, filesystems, snapshots)')
    list_.add_argument(
        '-H', action='store_true', dest='scriptable_mode',
        help='scripted mode (no headers, tab-delimited)')
    list_.add_argument(
        '-o', metavar='property[,property...]', dest='headers',
        type=PropertyList, help='comma-separated list of properties',
        default=PropertyList('name,used,available,refer,mountpoint'))

    promote = subparsers.add_parser(
        'promote', help='turn a cloned snapshot into a standalone filesystem')
    promote.add_argument('clone_filesystem')

    receive = subparsers.add_parser(
        'receive', help='create a new filesystem from "zzzfs send" output')
    receive.add_argument('filesystem')

    rename = subparsers.add_parser('rename', help='move or rename a dataset')
    rename.add_argument('identifier', metavar='filesystem|snapshot')
    rename.add_argument('other_identifier', metavar='filesystem|snapshot')

    rollback = subparsers.add_parser(
        'rollback', help='replace a filesystem with a snapshot')
    rollback.add_argument('snapshot')

    send = subparsers.add_parser(
        'send', help='serialize snapshot into a data stream')
    send.add_argument('snapshot')

    set_ = subparsers.add_parser(
        'set', help='set a property value for a dataset')
    set_.add_argument('keyval', metavar='property=value')
    set_.add_argument('identifiers', metavar='filesystem|snapshot', nargs='+')

    snap = subparsers.add_parser(
        'snapshot', help='create snapshots of filesystems')
    snap.add_argument('snapshots', metavar='filesystem@snapname', nargs='+')

    # generate dict of argument keys/values
    args = parser.parse_args(argv[1:])
    params = dict(args._get_kwargs())
    del params['command']

    retval = getattr(zfs, args.command)(**params)

    if type(retval) is str:
        return retval

    elif args.command not in ('diff', 'get', 'list', 'send'):
        # pool-modifying commands; log in pool history
        if isinstance(retval, Dataset):
            retval.pool.log_history_event(argv)
        else:
            # multiple affected datasets; only log command once per pool
            for pool_name in list(set(dataset.pool.name for dataset in retval)):
                Pool(pool_name).log_history_event(argv)


if __name__ == '__main__':
    try:
        output = zzzfs_main(sys.argv)
    except ZzzFSException, e:
        sys.exit('%s: %s' % (sys.argv[0], e))

    if output:
        print output
