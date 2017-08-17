# Copyright 2016 The Jackson Laboratory
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function

import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func

from managed_batch.model.base import Base
from managed_batch.model.job import Job
from managed_batch.model.status import Status
from managed_batch.model.pipeline import Pipeline

from managed_batch.model.session import Session


def initialize_model(db_path, echo_sql=False):
    """
    Create a connection to a new database.
    Record the session in the Session object.
    :param db_path: The full path to the database to be created.  For testing,
        you can specify :memory:
    :param echo_sql: If true, the SQL will be written to stdout (?err?) as it
        is executed.
    :return: The inintialized session

    NOTE: For some reason I haven't figured out, just setting the
    Session.session seems to set it only for this module and leave it None for
    the rest of the modules.  So we return it, and it is up to our caller to
    set it in the Session class.
    """
    engine = create_engine('sqlite:///{0}'.format(db_path))

    #Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine, checkfirst=True)
    session_func = sessionmaker(bind=engine)
    session = session_func()
    engine.echo = echo_sql

    logging.debug("Model initialization is complete.")
    logging.info("Using database {0}".format(
        db_path
    ))
    return session


def count_submitted_jobs():
    count_q = Session.query(Job).filter_by(status_id=Status.get_id('Submitted')).statement.with_only_columns([func.count()]).order_by(None)
    return Session.session.execute(count_q).scalar()


def mark_submitted(job, torque_id):
    logging.debug('Submitting: {0}'.format(job))
    job.set_status('Submitted')
    job.torque_id = torque_id
    logging.debug('Now in submitted state: {0}'.format(job))
    Session.session.commit()


def mark_complete_and_release_dependencies(job):
    # Now let's complete that job.
    logging.debug('Now completing job: {0}'.format(job.job_name))

    job.set_status('Complete')
    logging.debug('The now-completed job is: {0}'.format(job))

    # Find all the jobs depending on the completed job.
    dependent_jobs = Session.session.query(Job).filter(
        Job.depends_on.any(Job.id == job.id))
    for j in dependent_jobs:
        logging.debug('Found dependent job: {0}'.format(j))
        j.depends_on.remove(job)
        logging.debug("New state with completed job removed: {0}".format(j))
    Session.session.commit()


def scan_for_runnable_jobs(limit=None):
    """
    Cans the database for jobs that are eligible to run; in other words,
    those with an empty dependency list and the status "Not Submitted".
    :return: A list of runnable jobs.
    """
    unsubmitted_status_id = Status.get_id('Not Submitted')
    logging.debug('Finding runnable jobs')
    ready_jobs_query = Session.query(Job).filter(~Job.depends_on.any()). \
        filter_by(status_id=unsubmitted_status_id)
    if limit:
        ready_jobs_query = ready_jobs_query.limit(limit)
    ready_jobs = ready_jobs_query.all()
    if not ready_jobs:
        logging.debug('No jobs are ready to execute')
    else:
        logging.debug('The jobs that are ready to execute are:')
        for j in ready_jobs:
            logging.debug('    {0}'.format(j))
    return ready_jobs


def init_statuses():
    """
    Create database records for the statuses we need to track.  This is real
    code that can probably go into the final solution.
    :return: None
    """
    # Delete any previous records; we're initializing from scratch.

    logging.debug("In init_statuses(), session={0}".format(Session.session))
    # Create the statuses
    statuses = ['Not Submitted', 'Submitted', 'Complete', 'Failed', 'Deleted']
    logging.debug("Creating {0} statuses.  They are: {1}".format(
        len(statuses), statuses))

    for status in statuses:
        Session.add(Status(status))
    Session.commit()
    pass


def get_all_jobs():
    jobs = Session.query(Job).all()
    return jobs


