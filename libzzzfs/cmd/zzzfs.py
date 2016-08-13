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
from libzzzfs import zfs
from libzzzfs.dataset import Dataset, Pool
from libzzzfs.interpreter import ZzzfsCommandInterpreter
from libzzzfs.util import ZzzFSException


def zzzfs_main(argv):
    cmd = ZzzfsCommandInterpreter(argv[1:])

    if cmd.args.command is None:
        sys.exit(cmd.parser.print_usage())

    retval = getattr(zfs, cmd.args.command)(**cmd.params)

    if type(retval) is str:
        return retval

    elif cmd.args.command not in ('diff', 'get', 'list', 'send'):
        # pool-modifying commands; log in pool history
        if isinstance(retval, Dataset):
            retval.pool.log_history_event(argv)
        else:
            # multiple affected datasets; only log command once per pool
            for pool_name in list(set(dataset.pool.name for dataset in retval)):
                Pool(pool_name).log_history_event(argv)


def main():
    try:
        output = zzzfs_main(sys.argv)
    except ZzzFSException as e:
        sys.exit('%s: %s' % (sys.argv[0], e))

    if output:
        print(output)


if __name__ == '__main__':
    main()
