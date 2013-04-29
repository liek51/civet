#!/usr/bin/env python

import sys
import os
import job_runner.torque

def usage():
    print "Usage: " + sys.argv[0] + " <path_to_pipeline_log_dir>"

def main():
    if len(sys.argv) != 2:
        usage()
        return 1

    log_dir = sys.argv[1]
    
    # get listing of batch jobs from the pipeline's log directory
    # each line in batch_jobs is [batch_id, job_name, [dependencies]])
    batch_jobs = job_runner.torque.jobs_from_logdir(log_dir)
    
    print "Getting status for pipeline with log directory at:"
    print "\t{0}\n\n".format(log_dir)
    
    for job in batch_jobs:
        print "{0} ({1}):".format(job[1], job[0])
        if os.path.exists(os.path.join(log_dir, job[1] + "-status.txt")): 
            status = job_runner.torque.get_status_from_file(log_dir, job[1])
            print "\tExit Status={0}".format(status['exit_status'])
            print "\tWalltime={0}".format(status['walltime'])
        else:
            status = job_runner.torque.query_job(job[0])
            if status:
                print "\tState={0}".format(status.state)
                if status.state == 'R' or status.state == 'C':
                    print "\tWalltime={0}".format(status.walltime)
                elif status.state == 'H':
                    print "\tDepends on {0}".format(job[2])
            else:
                print "Error querying pbs_server for job {0}".format(job[0])

if __name__ == '__main__':
    main()