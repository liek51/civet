
import os

BATCH_ID_LOG = "pipeline_batch_id_list.txt"
JOB_STATUS_SUFFIX = "-status.txt"

def get_status_from_file(logdir, job_name):
    return dict(line.strip().split('=') for line in open(os.path.join(logdir, job_name + "-status.txt")))
    
def jobs_from_logdir(logdir):
    batch_jobs = []
    for line in open(os.path.join(logdir, BATCH_ID_LOG)):
        batch_jobs.append(line.strip().split('\t'))
        
    return batch_jobs
