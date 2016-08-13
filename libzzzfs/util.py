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


def validate_component_name(component_name, allow_slashes=False):
    '''Check that component name starts with an alphanumeric character, and
    disalllow all non-alphanumeric characters except underscore, hyphen, colon,
    and period in component names.
    '''
    allowed = ('_', '-', ':', '.')
    if allow_slashes:
        allowed += ('/',)

    if len(component_name) == 0:
        return False
    if not component_name[0].isalnum():
        return False
    for c in component_name:
        if c not in allowed and not c.isalnum():
            return False

    return True


class ZzzFSException(Exception):
    pass


class PropertyList(object):
    # Numeric columns are right-aligned when tabulated.
    numeric_types = ['alloc', 'avail', 'cap', 'free', 'refer', 'size', 'used']

    # synonymous field names
    shorthand = {'available': 'avail', 'capacity': 'cap'}

    def __str__(self):
        return ','.join(self.items)

    def __repr__(self):
        return '<%s: %s>' % (self.__class__.__name__, str(self))

    def __init__(self, user_string):
        self.items = user_string.split(',')

    def validate_against(self, acceptable):
        # compare against a set of acceptable fields
        for col in self.names:
            if col not in acceptable:
                raise ZzzFSException('%s: unrecognized property name' % col)

    @property
    def names(self):
        # use shorthand name, if any, as canonical name
        for col in self.items:
            yield self.shorthand.get(col, col)

    @property
    def types(self):
        # strings unless explicitly numeric
        for col in self.names:
            if col in self.numeric_types:
                yield int
            else:
                yield str


class PropertyAssignment(object):
    '''property=value command-line argument, as used in zzzfs set command.'''
    def __init__(self, user_string):
        try:
            self.key, self.val = user_string.split('=')
        except ValueError:
            raise ZzzFSException(
                '%r: invalid property=value format' % user_string)

        if not validate_component_name(self.key):
            raise ZzzFSException('%s: invalid property' % self.key)

        self.user_string = user_string

    def __str__(self):
        return self.user_string


def tabulated(data, headers, scriptable_mode=False, sort_asc=[], sort_desc=[]):
    '''Generates a printable table as a string given data (an array of dicts)
    and an array of field names for headers.
    '''
    if len(data) == 0:
        return ''

    types = list(headers.types)
    names = list(headers.names)

    row_format = '\t'.join('%s' for i in range(len(names)))
    if not scriptable_mode:
        # For evenly-spaced columns, left-align each text field (right-align
        # each numeric field) in a cell that's big enough for the longest value
        # in each column.
        data_and_headers = data + [dict(zip(names, names))]
        cells = []
        for i in range(len(names)):
            box_width = max(len(r.get(names[i]) or '-') for r in data_and_headers)
            if types[i] == str:
                box_width *= -1  # negative field width means left-align
            cells.append('%%%ds' % box_width)
        row_format = '\t'.join(cells)

    # sort by specified fields, if any
    for field in sort_asc + sort_desc:
        if field not in names:
            raise ZzzFSException('%s: no such column' % field)

    for field in sort_asc:
        data = sorted(data, key=lambda row: row[field])

    for field in sort_desc:
        data = list(reversed(sorted(data, key=lambda row: row[field])))

    # Add individual data rows.
    output = '\n'.join(
        row_format % tuple(row.get(names[i]) or '-' for i in range(len(names)))
        for row in data)

    # Prepend header row in all caps.
    if not scriptable_mode:
        output = row_format % tuple(h.upper() for h in names) + '\n' + output

    return output
