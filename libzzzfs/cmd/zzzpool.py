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

from libzzzfs import zpool
from libzzzfs.dataset import Pool, ZzzFSException
from libzzzfs.util import PropertyList


def zzzpool_main(argv):
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='command', title='subcommands')

    # per-command arguments
    create = subparsers.add_parser('create', help='create a pool')
    create.add_argument('pool_name', metavar='pool', help='pool name')
    create.add_argument('disk', help='directory in which to create pool')

    destroy = subparsers.add_parser('destroy', help='destroy a pool')
    destroy.add_argument('pool_name', metavar='pool', help='pool name')

    history = subparsers.add_parser(
        'history', help='display pool command history')
    history.add_argument(
        'pool_names', metavar='pool', nargs='*', default=[], help='pool name')
    history.add_argument(
        '-l', action='store_true', dest='long_format',
        help='show log records in long format')

    list_ = subparsers.add_parser('list', help='list pools and properties')
    list_.add_argument('pool_name', nargs='?', default=None, help='pool name')
    list_.add_argument(
        '-H', action='store_true', dest='scriptable_mode',
        help='scripted mode (no headers, tab-delimited)')
    list_.add_argument(
        '-o', metavar='property[,...]', type=PropertyList, dest='headers',
        default=PropertyList('name,size,alloc,free,cap,health,altroot'),
        help='comma-separated list of properties')

    # generate dict of argument keys/values
    args = parser.parse_args(argv[1:])
    params = dict(args._get_kwargs())
    del params['command']

    if args.command is None:
        sys.exit(parser.print_usage())

    retval = getattr(zpool, args.command)(**params)
    if type(retval) is str:
        return retval

    if isinstance(retval, Pool) and args.command == 'create':
        retval.log_history_event(argv)


def main():
    try:
        output = zzzpool_main(sys.argv)
    except ZzzFSException as e:
        sys.exit('%s: %s' % (sys.argv[0], e))

    if output:
        print(output)


if __name__ == '__main__':
    main()
