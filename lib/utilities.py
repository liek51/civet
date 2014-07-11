#
# Utilities needed by multiple parts of the pipeline.

import os
import errno
import sys

def make_sure_path_exists(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            print >> sys.stderr, 'Error while creating directory', path
            raise


def parse_delay_string(delay):
    split_string = delay.split(':')
    assert len(split_string) == 2 or len(split_string) == 1, "Delay must be in format '[[h]h:][m]m'"
    if len(split_string) == 2:
        hours = int(split_string[0])
        minutes = int(split_string[1])
    else:
        hours = 0
        minutes = int(split_string[0])

    return hours, minutes
