#!/usr/bin/env python
#
# Command line interface to the validity capabilities. Two capabilities
#  1. Create a "golden images" file, given a list of files
#  2. Validate a list of file against the golden record.
#

import argparse
import sys
import os

import validity

def process_options():

    #DEFAULT_MASTER_LIST='/opt/jax/cga/master_file_list'
    DEFAULT_MASTER_LIST='./fred/master_file_list'

    parser = argparse.ArgumentParser('Validate files')
    parser.add_argument('-g', '--generate', dest='gen', action='store_true',
        default=False, help='Generate a file containing info about the '
                            'current state of the files.')
    parser.add_argument('-m', '--master-list', dest='master_list',
                        default=DEFAULT_MASTER_LIST,
                        help='Specify an alternate file when creating '
                             'the master list.')
    parser.add_argument('files', metavar='F', type=str, nargs='+',
                        help='List of files to validate.')
    args = parser.parse_args()
    if not args.gen:
        if args.master_list != DEFAULT_MASTER_LIST:
            print >> sys.stderr, ('Option --master list only available when '
                'generating the master list. Ignoring.')
            args.master_list = DEFAULT_MASTER_LIST
    return args

def ensure_directory_exists(path):
    dir = os.path.split(path)[0]
    if not dir:
        dir = '.'
    try:
        if not os.path.exists(dir):
            os.mkdir(dir)
    except:
        print >> sys.stderr, 'Could not create directory', dir, '\nExiting...'
        sys.exit(1)
    
def main():
    args = process_options()

    files = validity.FileCollection()
    for fn in args.files:
        files.add_file(fn)
    
    if args.gen:
        #Generate list.
        f = args.master_list
        ensure_directory_exists(f)
        files.to_JSON_file(args.master_list)
    else:
        # Validate files.
        # Read in the master
        m = validity.FileCollection()
        m.from_JSON_file(args.master_list)
        validation_failures = files.validate(m)
        if validation_failures:
            print >> sys.stderr, validation_failures
            sys.exit(1)
    sys.exit(0)

main()