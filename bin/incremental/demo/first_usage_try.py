#! /usr/bin/env python
from __future__ import print_function

import logging
import time
import inspect
import os
import sys

# add some magic so when we run this without PYTHONPATH it can find the project
# lib dir (this usually happens when testing without using a Civet modulefile)
cmd_folder = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile(inspect.currentframe()))[0]))
lib_folder = os.path.join(cmd_folder, '../../../lib')
if lib_folder not in sys.path:
    sys.path.insert(0, lib_folder)

from managed_batch.model.job import Job
from managed_batch.model.pipeline import Pipeline
from managed_batch.model.session import Session
from managed_batch.model.status import Status

from managed_batch.controller.utilities import get_all_jobs, initialize_model, \
    init_statuses, mark_complete_and_release_dependencies, mark_submitted, \
    scan_for_runnable_jobs


def scan_for_complete():
    """
    Demo routine for getting a list of complete jobs.  This will be replaced
    by a routine implementing the civet_status completeness tests.
    For this demo, we simply mark all the submitted jobs as complete.
    :return: A list of complete jobs.
    """
    submitted_status_id = Status.get_id('Submitted')
    complete_status_id = Status.get_id('Complete')
    submitted_jobs = Session.query(Job).filter_by(
        status_id=submitted_status_id).all()
    logging.debug('Submitted jobs which have just completed:')
    for job in submitted_jobs:
        job.status_id = complete_status_id
        logging.debug('    {0}'.format(job))
    Session.commit()
    return submitted_jobs


def create_demo_jobs():
    """
    Create a pipeline and four demonstration job records.
    This is for demo purposes only.

    Job constructor is:
        def __init__(self, pipeline, job_name, threads, stdout_path,
                 stderr_path, script_path, epilog_path, mem,
                 email_list, mail_options, env, depends_on):

    :return: None
    """
    # Create the pipeline
    pipeline = Pipeline('My pipeline', 'Path to log dir')
    logging.debug('Created new pipeline: {0}'.format(pipeline))

    # Two "start" jobs with no dependencies
    logging.debug('\nNow creating four jobs.  Job 3 depends on jobs 1 and 2, '
                  'and job 4 depends on job 1 and job 3.')
    j1 = Job(pipeline, 'Job_1', 1, 'Stdout path 1',
             'Stderr path 1', 'Path to script 1', 'Epilog_path 1', 64,
             'Email addr 1', 'Mail opts 1', 'Env 1', [])
    j2 = Job(pipeline, 'Job_2', 2, 'Stdout path 2',
             'Stderr path 2', 'Path to script 2', 'Epilog_path 2', 128,
             'Email addr 2', 'Mail opts 2', 'Env 2', [])

    # Have to put these into the session and commit them to get their IDs.
    Session.add(j1)
    Session.add(j2)
    Session.commit()

    # Now two dependent jobs
    j3 = Job(pipeline, 'Job_3', 4, 'Stdout path 3',
             'Stderr path 3', 'Path to script 3', 'Epilog_path 3', 128,
             'Email addr 3', 'Mail opts 3', 'Env 3', [j1, j2])
    Session.add(j3)
    Session.commit()

    j4 = Job(pipeline, 'Job_4', 8, 'Stdout path 4',
             'Stderr path 4', 'Path to script 4', 'Epilog_path 4', 256,
             'Email addr 4', 'Mail opts 4', 'Env 4', [j1, j3])

    Session.add(j4)
    Session.commit()

    logging.debug('Pipeline status: {0}'.format(pipeline))
    logging.debug('Done creating pipeline and jobs.')

    # Demonstrate getting a log directory from a Job.
    logging.debug('Job 1\'s log directory is "{0}"'.format(
        j1.pipeline.log_directory
    ))

def main_loop():
    pipeline = Session.query(Pipeline).one()
    while True:
        if pipeline.is_complete():
            logging.debug('Final status of the pipeline is:{0}'.format(pipeline))
            break
        logging.debug('The status of the pipeline is:{0}'.format(pipeline))
        runnable = scan_for_runnable_jobs()
        for job in runnable:
            mark_submitted(job, demo_job_prefix + str(job.id))

        time.sleep(1)

        complete = scan_for_complete()
        for job in complete:
            mark_complete_and_release_dependencies(
                demo_job_prefix + str(job.id))

        all_jobs = get_all_jobs()
        logging.debug("The current status of all jobs is:")
        for job in all_jobs:
            logging.debug(job)

demo_job_prefix = 'cadillac:'

def main():
    logging.basicConfig(level=logging.DEBUG)
    logging.debug("Initializing the model.")
    Session.session = initialize_model('civet.db')
    logging.debug("Creating status records")
    init_statuses()
    logging.debug("Retrieving statuses from DB")
    # Now get the statuses back out of the DB.
    result = Session.query(Status)
    for row in result:
        logging.debug(row)

    logging.debug("\nCreating demo jobs")
    create_demo_jobs()
    all_jobs = get_all_jobs()

    for j in all_jobs:
        logging.debug(j)

    logging.debug('\nEntering main_loop')
    main_loop()
    logging.debug("Done!")


if __name__ == '__main__':
    main()