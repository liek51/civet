#! /usr/bin/env python

#The "main" program of the exome tumor only pipeline.

import sys
import os
import pipeline_parse as PL
def main():
    if len(sys.argv) != 3:
        print >> sys.stderr, ('The single sample (tumor only) exome pipeline '
                              'requires two arguments: fastq-end-1, fastq-end-2.')
        sys.exit(1)

    # Determine the path to the pipeline...
    pathname = os.path.dirname(sys.argv[0])
    fullpath = os.path.abspath(pathname)
    pipeline = os.path.join(fullpath, '../exome/single_sample_exome.xml')
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
