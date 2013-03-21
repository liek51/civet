#! /usr/bin/env python

# This is an alternate mail program for the pipeline code, that 
# collects all the files that need to be validated, and 
# generates a json file with their data.  The created file
# must be moved into the proper directory for the pipeline's use.

# For compatibility with the pipeline XML, this command must be invoked
# with the same parameters as a regular pipeline invocation.

import sys
import pipeline_parse as PL
import validity

def main():
    PL.parse_XML(sys.argv[1], sys.argv[2:])
    fns = PL.collect_files_to_validate()
    files = validity.FileCollection()
    for fn in fns:
        files.add_file(fn)
    name = PL.name
    name += "_file_data_for_validation.json"

    files.to_JSON_file(name)

if __name__ == "__main__":
    main()
