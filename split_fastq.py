#! /bin/env python

# split_fastq.py
# Given a integer indicating the number of fragments and a fastq file, 
# split the incoming fastq file into N parts, as evenly as possible. The
# last fragment may be up to N-1 fastq records longer than the other
# fragments.
#
# The output files 
import sys
import os
import argparse

def parse_options():
    parser = argparse.ArgumentParser(description='Fastq file splitter')
    parser.add_argument('--dir', '-d')
    parser.add_argument('nFragments', type=int)
    parser.add_argument('fastq')
    args = parser.parse_args()
    print args
    return args

def getLength(f):
    n = 0
    for line in open(f):
        n += 1
    entries = n / 4
    if entries * 4 != n:
        print >> sys.stderr, "Woah! the file isn't a valid fastq file; not a multiple of 4 lines! (" + str(n), 'lines)'
        sys.exit(1)
    return entries

def fileNameTemplate(dir, fn):
    root, ext = os.path.splitext(fn)
    fn = root + '_%02d' + ext
    if dir:
        fn = os.path.join(dir, fn)
    return fn

def processFragment(f, nEntries, ofn):
    of = open(ofn, 'w')
    # Entries are groups of 4 lines each...
    for n in range(nEntries * 4):
        line = f.readline()
        if len(line) == 0:
            print >> sys.stderr, "ERROR! Input file prematurely exhausted."
            sys.exit(1)
        print >> of, line,
    of.close()

def main():
    args = parse_options()
    try:
        entries = getLength(args.fastq)
    except:
        print >> sys.stderr, "ERROR: unable to open input file:", args.fastq
        sys.exit(1)

    template = fileNameTemplate(args.dir, args.fastq)
    f = open(args.fastq)
    entriesPerFile = entries / args.nFragments
    for n in range(args.nFragments - 1):
        ofn = template % (n)
        processFragment(f, entriesPerFile, ofn)

    # Now we have to do the last fragment, which may be a little longer.
    lastFragEntries = entriesPerFile + (entries - (entriesPerFile * args.nFragments))
    ofn = template % (args.nFragments - 1)
    processFragment(f, lastFragEntries, ofn)
    
    # Make sure we've now exhausted the input file.
    line = f.readline()
    if len(line) != 0:
        print >> sys.stderr, "ERROR: input file not completely processed."
        sys.exit(1)
        
main()
