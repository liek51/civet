#
# Copyright (C) 2016  The Jackson Laboratory
#
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

# Utilities needed by multiple parts of the pipeline.

from __future__ import print_function

import os
import errno
import sys
import unicodedata


def make_sure_path_exists(path, mode=None):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            print('Error while creating directory ' + path, file=sys.stderr)
            raise
    if mode:
        os.chmod(path, mode)


def parse_delay_string(delay):
    split_string = delay.split(':')
    if len(split_string) != 1 and len(split_string) != 2:
        raise ValueError("Delay must be in format '[[h]h:][m]m'")

    if len(split_string) == 2:
        hours = int(split_string[0])
        minutes = int(split_string[1])
    else:
        hours = 0
        minutes = int(split_string[0])

    return hours, minutes


def cleanup_command_line():

    if not sys.stdin.encoding or sys.stdin.encoding == 'ascii':
        return

    conversion_pairs = {
        'EN DASH': '-',
        'EM DASH': '--',
        'LEFT DOUBLE QUOTATION MARK': '"',
        'RIGHT DOUBLE QUOTATION MARK': '"',
        'LEFT SINGLE QUOTATION MARK': "'",
        'RIGHT SINGLE QUOTATION MARK': "'",
    }

    for i in range(len(sys.argv)):
        # create a unicode string with the decoded contents of the corresponding
        # sys.argv string
        decoded = unicode(sys.argv[i], sys.stdin.encoding)
        for key,val in conversion_pairs.iteritems():
            decoded = unicode.replace(decoded, unicodedata.lookup(key), val)
        # Should we be doing 'strict' here instead of 'replace'?
        sys.argv[i] = decoded.encode(sys.stdin.encoding, 'replace')
