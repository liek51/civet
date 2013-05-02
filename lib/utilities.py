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

