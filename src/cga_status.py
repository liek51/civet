#!/usr/bin/env python

import sys
import os
import job_runner.torque
import PBSQuery

# need to reuse the same PBSQuery instance otherwise garbage collection doesn't happen fast enough 
# and we seem to run out of connections on pbs_server for larger pipelines.
pbsq = PBSQuery.PBSQuery()


def usage():
    print "Usage: " + sys.argv[0] + " <path_to_pipeline_log_dir>"

def query_job(id):
	"""
		Query server for status of job specified by id.
		
		query_job will return None if the job does not exist on the server, 
		otherwise it will return a JobStatus object.
	""" 
	job_status =  pbsq.getjob(id)
	# check to see if the job existed.  this is kind of lame, but we can't
	# just do "if job_status:" because PBSQuery.getjob returns an empty 
	# dictionary if the job is not found, but it returns some other object
	# that acts like a dictionary but does not have a __nonzero__ attribute
	if 'Job_Name' in job_status:
		return job_runner.torque.JobStatus(job_status)
	else:
		return None

def main():
    if len(sys.argv) != 2:
        usage()
        return 1

    log_dir = sys.argv[1]
    
    # get listing of batch jobs from the pipeline's log directory
    # each line in batch_jobs is [batch_id, job_name, [dependencies]])
    batch_jobs = job_runner.torque.jobs_from_logdir(log_dir)
    
    print "\n\nGetting status for pipeline with log directory at:"
    print "\t{0}\n\n".format(log_dir)
    
    # check for the abort.log -- this will indicate something went wrong with
    # the run
    if os.path.exists(os.path.join(log_dir, "abort.log")):
        print "Warning: Pipeline aborted due to non-zero exit value of at least one job.  Details below."
    
    complete_jobs = 0
    running_jobs = 0
    pending_jobs = 0
    unknown_state = 0
    total_jobs = len(batch_jobs)
    
    for job in batch_jobs:
        print "{0} ({1}):".format(job[1], job[0])
        if os.path.exists(os.path.join(log_dir, job[1] + "-status.txt")): 
            status = job_runner.torque.get_status_from_file(log_dir, job[1])
            print "\tExit Status={0}".format(status['exit_status'])
            print "\tWalltime={0}".format(status['walltime'])
            print "\tWalltime(Requested)={0}".format(status['requested_walltime'])
            complete_jobs += 1
        else:
            status = query_job(job[0])
            if status:
                print "\tState={0}".format(status.state)
                if status.state == 'R' or status.state == 'C':
                    print "\tWalltime={0}".format(status.walltime)
                    print "\tWalltime(Requested)={0}".format(status.requested_walltime)
                    if status.state == 'R':
                        running_jobs += 1
                    else:
                        complete_jobs += 1
                elif status.state == 'H':
                    print "\tWalltime(Requested)={0}".format(status.requested_walltime)
                    print "\tDepends on {0}".format(job[2])
                    pending_jobs += 1
            else:
                print "\tError querying pbs_server for job {0}.  Job may have been deleted.".format(job[0])
                unknown_state += 1
                
    print "\n\nSummary: "
    print "Total Pipeline Jobs: {0}".format(total_jobs)
    print "\tCompleted Jobs: {0}".format(complete_jobs)
    print "\tRunning Jobs: {0}".format(running_jobs)
    print "\tPending Jobs: {0}".format(pending_jobs)
    if unknown_state:
        print "\t{0} jobs in an unknown state (status file not found and error querying pbs_server)".format(unknown_state)

if __name__ == '__main__':
    main()