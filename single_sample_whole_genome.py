#! /usr/bin/env python

#The "main" program of the whole genome tumor only pipeline.

import os, sys, inspect
cmd_folder = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile( inspect.currentframe() ))[0]))
lib_folder = os.path.join(cmd_folder, 'src')
if lib_folder not in sys.path:
     sys.path.insert(0, lib_folder)

import pipeline_parse as PL
def main():
    global cmd_folder
    if len(sys.argv) != 3:
        print >> sys.stderr, ('The single sample (tumor only) whole genome pipeline '
                              'requires two arguments: fastq-end-1, fastq-end-2.')
        sys.exit(1)

    # Determine the path to the pipeline...
    pipeline = os.path.join(cmd_folder, 'whole_genome/single_sample_whole_genome.xml')
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