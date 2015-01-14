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

from dataset import Pool
from util import PropertyList, tabulated, ZzzFSException


def create(pool_name, disk):
    '''Add a pool in the specified directory.'''
    if os.path.exists(disk) and len(os.listdir(disk)) != 0:
        raise ZzzFSException, '%s: disk in use' % pool_name

    pool = Pool(pool_name, should_exist=False)
    pool.create(os.path.abspath(disk))
    return pool


def destroy(pool_name):
    '''Remove a pool.'''
    Pool(pool_name, should_exist=True).destroy()


def history(pool_names=[], long_format=False):
    pools = Pool.all()
    if pool_names:
        pools = [Pool(p, should_exist=True) for p in pool_names]

    output = []
    for pool in pools:
        output.append('History for %r:' % pool.name)
        output += pool.get_history(long_format)

    return '\n'.join(output)


def list(pool_name=None,
         headers=PropertyList('name,size,alloc,free,cap,health,altroot'),
         scriptable_mode=False):
    '''List all pools.'''
    headers.validate_against([
        'name', 'size', 'alloc', 'free', 'cap', 'health', 'altroot'])

    pools = Pool.all()
    if pool_name:
        pools = [Pool(pool_name, should_exist=True)]

    return tabulated(
        [{'name': p.name, 'health': 'ONLINE'} for p in pools], headers,
        scriptable_mode)
