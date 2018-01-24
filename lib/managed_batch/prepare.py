from __future__ import print_function

import sys
import os
import logging

from model.session import Session
from model.file_info import FileInfo
from model.job import Job
from model.pipeline import Pipeline

from controller.utilities import initialize_model


def initialize_task_file(file_path):
    append_mode = os.path.exists(file_path)

    try:
        Session.session = initialize_model(file_path)
    except Exception as e:
        logging.exception("Error opening task database: {}".format(e.message))
        print("Error opening task database: {}".format(e.message),
              file=sys.stderr)
        sys.exit(3)

    if not append_mode:
        logging.debug("Not in append mode. Initializing.")
        # Initialize the file_info table in the database.
        FileInfo()
    else:
        logging.debug("In append mode.")

        # make sure the task file is the correct schema and hasn't already
        # been submitted
        if not FileInfo.is_current_schema():
            msg = "Task database {} is not the current schema version, and " \
                  "cannot be used.".format(file_path)
            logging.error(msg)
            print(msg, file=sys.stderr)
            sys.exit(4)

        if FileInfo.is_started():
            logging.error("Unable to add tasks to task file {}\n"
                          "\tTask file has already been submitted.".format(
                              file_path))
            print("\tUnable to add tasks to task file {}\n"
                  "\t\tTask file has already been submitted.".format(
                      file_path), file=sys.stderr)
            sys.exit(5)


def insert_tasks(PL, task_file):

    pipeline = Pipeline(PL.name, PL.log_dir)
    logging.debug("Pipeline is: {}".format(pipeline))

    task_list = PL.prepare_managed_tasks()
    logging.debug("Task list is: {}".format([x['name'] for x in task_list]))

    # we need to be able to translate the dependencies as stored in the task
    # list (list of other task names that a particular task depends on)
    # into a list of Job object references that have already been added to the
    # session. We will build up a dictionary of task['name'] : Job as we
    # insert them
    deps_to_job = {}
    print("  Inserting tasks into {}".format(task_file))
    logging.info("Inserting tasks into {}".format(task_file))
    try:
        for task in task_list:
            print("    -> {}".format(task['name']))
            try:
                dependencies = [deps_to_job[d] for d in task['dependencies']]
            except KeyError as e:
                logging.exception("Key error processing dependencies")
                msg = "Task {} depends on a task that hasn't been been " \
                      "processed ({}). Check your Pipeline XML".format(
                          task['name'], e.args[0])
                raise Exception(msg)
            job = Job(pipeline, task['name'], task['threads'],
                      task['stdout_path'], task['stderr_path'],
                      task['script_path'], task['epilogue_path'],
                      task['mem'], task['email_list'], task['mail_options'],
                      task['batch_env'], dependencies, task['queue'],
                      task['walltime'])

            deps_to_job[task['name']] = job
            logging.debug("Adding job {} (log dir: {}) to session".format(
                job.job_name, job.pipeline.log_directory))
            Session.add(job)
    except Exception as e:
        logging.exception("Error inserting tasks into database")
        print("Error inserting tasks into database: {}".format(e),
              file=sys.stderr)
        sys.exit(6)

    # only commit the session if we were able to add all the jobs to the session
    # without catching an Exception
    Session.commit()

    logging.info("  {} tasks have been inserted into task file {}; "
                 "(log dir: {})".format(
        len(task_list), task_file, PL.log_dir))

    return len(task_list)

