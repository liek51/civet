
import os
import inspect

BATCH_ID_LOG = "pipeline_batch_id_list.txt"
JOB_STATUS_SUFFIX = "-status.txt"
CANCEL_LOG_FILENAME = "cancel.log"

CIVET_HOME = os.path.normpath(os.path.join(os.path.realpath(os.path.abspath(os.path.split(inspect.getfile( inspect.currentframe() ))[0])), "../../"))

def get_status_from_file(logdir, job_name):
    return dict(line.strip().split('=') for line in open(os.path.join(logdir, job_name + "-status.txt")))
    
def jobs_from_logdir(logdir):
    batch_jobs = []
    for line in open(os.path.join(logdir, BATCH_ID_LOG)):
        batch_jobs.append(line.strip().split('\t'))
        
    return batch_jobs
