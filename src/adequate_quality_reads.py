#! /usr/bin/env python

# adequate_quality_reads.py
# A quick script to parse the quality statistics file to determine if
# it is worthwhile continuing the pipeline run.

# Two parameters:
#   The statistics file to parse
#   The minimum portion of high quality, filtered reads. If the last
#   character is '%', treat as a percentage (divide by 100). Otherwise
#   treat as a simple float 0<=X<=1.  If out of range, error out.

# Returns:  0 if adequate hq filtered reads.
#           1 if inadequate hq filtered reads.
#           2 if other error.

import sys

def str_to_float(s):
    try:
        if s[-1] == '%':
            v = float(s[:-1])/100
        else:
            v = float(s)
    except:
        print >> sys.stderr, 'Invalid floating point number,', s
        sys.exit(2)

    if v < 0.0 or v > 1.0:
        print >> sys.stderr, 'Value must be between 0.0 and 1.0:', v
        sys.exit(2)

    return v

if len(sys.argv) != 3:
    print >> sys.stderr, 'USAGE:', sys.argv[0], 'qual-stats-file  cutoff'
    sys.exit(2)

# Get our quality cutoff threshhold.
cutoff = str_to_float(sys.argv[2])

try:
    for line in open(sys.argv[1]):
        line = line.rstrip()
        if not 'Percentage of HQ filtered reads' in line:
            continue

        # We've found the line we're interested in.
        parts = line.split()
        # If we're handling paired end, the last two fields will be 
        # percentages, and should be the same.  If single end, 
        # only handle the last field.
        one = str_to_float(parts[-1])
        if '%' in parts[-2]:
            other = str_to_float(parts[-2])
            if one != other:
                print >> sys.stderr, 'Unexpected condition. Percentages are not equal.', one, other
                sys.exit(2)
        if one < cutoff:
            sys.exit(1)
        # Enough good reads. We're happy.
        sys.exit(0)
    #
    # If we got here, we didn't find the right line. Something's wrong. Fail.
    sys.exit(2)

except :
    # Not sure what went wrong... Bail.
    #print >> sys.stderr, 'Unexpected error:', sys.exc_info[0]
    #sys.exit(2)
    raise
