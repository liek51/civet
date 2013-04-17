#! /usr/bin/env python

#The "main" program of the exome tumor only pipeline.

import os, sys, inspect
cmd_folder = os.path.realpath(os.path.abspath(os.path.split(
                              inspect.getfile( inspect.currentframe() ))[0]))
lib_folder = os.path.join(cmd_folder, 'src')
if lib_folder not in sys.path:
     sys.path.insert(0, lib_folder)

import pipeline_parse as PL
def main():
    global cmd_folder
    if len(sys.argv) != 5:
        print >> sys.stderr, ('USAGE: ' + sys.argv[0] + ' tumor-end-1.fastq tumor-end-2.fastq'
                              ' normal-end-1.fastq normal-end-2.fastq')
        sys.exit(1)

    # Determine the path to the pipeline...
    pipeline = os.path.join(cmd_folder, 'exome/tumor_normal_exome.xml')
    try:
        with open(pipeline) as p:
            pass
    except IOError:
        print >> sys.stderr, 'Cannot open pipeline description file: ', pipeline
    else:
        PL.parse_XML(pipeline, sys.argv[1:])
        PL.submit()

if __name__ == "__main__":
    main()
