#! /usr/bin/env python

"""
Convert a target or bait bed file in gatk format, with no headers and 3
columns, into picard format with SAM headers and 5 columns.
"""

import sys
import os

if len(sys.argv) != 2:
    print >> sys.stderr, 'USAGE:', sys.argv[0], 'gatk-formatted-bed-file'
    print >> sys.stderr, '    produces outputfile ending with "_picard.bed"'
    sys.exit(1)

ifn = sys.argv[1]
base = os.path.split(ifn)[1]
if base[-4:] != '.bed':
    print >> sys.stderr, 'Input file does not end in ".bed". Exiting...'
    sys.exit(1)
ofn = ifn[:-4] + '_picard.bed'

of = open(ofn, 'w')

header="""@HD     VN:1.4  SO:coordinate
@SQ     SN:chrM LN:16571
@SQ     SN:chr1 LN:249250621
@SQ     SN:chr2 LN:243199373
@SQ     SN:chr3 LN:198022430
@SQ     SN:chr4 LN:191154276
@SQ     SN:chr5 LN:180915260
@SQ     SN:chr6 LN:171115067
@SQ     SN:chr7 LN:159138663
@SQ     SN:chr8 LN:146364022
@SQ     SN:chr9 LN:141213431
@SQ     SN:chr10        LN:135534747
@SQ     SN:chr11        LN:135006516
@SQ     SN:chr12        LN:133851895
@SQ     SN:chr13        LN:115169878
@SQ     SN:chr14        LN:107349540
@SQ     SN:chr15        LN:102531392
@SQ     SN:chr16        LN:90354753
@SQ     SN:chr17        LN:81195210
@SQ     SN:chr18        LN:78077248
@SQ     SN:chr19        LN:59128983
@SQ     SN:chr20        LN:63025520
@SQ     SN:chr21        LN:48129895
@SQ     SN:chr22        LN:51304566
@SQ     SN:chrX LN:155270560
@SQ     SN:chrY LN:59373566
@RG     ID:371panel     PL:ILLUMINA     LB:LI371panel  SM:371panel
@PG     ID:bwa  PN:bwa  VN:0.5.9-r26-dev"""

# Print out the header.
print >> of, header

for line in open(ifn):
	# There shouldn't be any header in this input, but skip it in case,
	# So that we don't have two copies.
    if line[0] == '@':
        continue
    parts = line.rstrip().split('\t')
	
	# Drop any NT_ contigs
    if "_" in parts[0]:
        continue

    parts.append('+')
    parts.append('--')
    print >> of, '\t'.join(parts)

of.close()
