#! /usr/bin/env python

#The "main" program of the development pipeline.

import sys
import pipeline_parse as PL
def main():
    # The name of the pipeline description is passed on the command line.
    #
    # This is a sample.  Requires four args.  Real one would take a variable
    # list.
    if len(sys.argv) < 5:
        print >> sys.stderr, 'This test version requires four arguments: XML, input1, input2, output-dir.'
        sys.exit(1)

    PL.parse_XML(sys.argv[1], sys.argv[2:])
    PL.submit()

if __name__ == "__main__":
    main()
