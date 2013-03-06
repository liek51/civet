#! /usr/bin/env python

#The "main" program of the development pipeline.

import sys
import pipeline_parse as PL
def main():
    # The name of the pipeline description is passed on the command line.
    #
    # This is a sample.  Requires two args.  Real one would take a variable
    # list.
    if len(sys.argv) < 3:
        print >> sys.stderr, "This test version requires two arguments: XML, input file."
        sys.exit(1)

    print dir(PL)
    PL.parse_XML(sys.argv[1], sys.argv[2:])
    print 'back from parse'
    PL.submit()

if __name__ == "__main__":
    main()
