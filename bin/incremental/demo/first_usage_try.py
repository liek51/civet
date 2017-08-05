#! /usr/bin/env python
from __future__ import print_function

import time

from sqlalchemy import create_engine
from sqlalchemy.orm import relationship, backref, sessionmaker

from base import Base


from job import Job
from status import Status


def mark_submitted(job, torque_id):
    # Fake submitting a job:
    submitted_status_id = get_status_id('Submitted')

    print('\nSubmitting:', job)
    job.status_id = submitted_status_id
    job.torque_id = torque_id
    print('Now in submitted state:', job)
    session.commit()


def mark_complete_and_release_dependencies(torque_id):
    # Now let's complete that job.
    print('\nNow completing torque_id:', torque_id)

    # Get the status ID for complete jobs, so we can mark it.
    complete_status_id = get_status_id('Complete')

    completed_job = session.query(Job).filter(Job.torque_id == torque_id).one()
    completed_job.status_id = complete_status_id
    print('\nThe now-completed job is:', completed_job)

    # Find all the jobs depending on the completed job.
    dependent_jobs = session.query(Job).filter(
        Job.dependencies.any(Job.torque_id == torque_id))
    for j in dependent_jobs:
        print('\nFound dependent job', j)
        print('Dependencies:', j.dependencies)
        j.dependencies.remove(completed_job)
        print("New dependency list:", j.dependencies)
    session.commit()


def scan_for_runnable_jobs():
    """
    Cans the database for jobs that are eligible to run; in other words,
    those with an empty dependency list and the status "Not Submitted".
    :return: A list of runnable jobs.
    """
    unsubmitted_status_id = get_status_id('Not Submitted')
    print('\nFinding runnable jobs')
    ready_jobs = session.query(Job).filter(~Job.dependencies.any()). \
        filter_by(status_id=unsubmitted_status_id).all()
    print('The jobs that are ready to execute are:')
    if not ready_jobs:
        print('No jobs are ready to execute')
    for j in ready_jobs:
        print('   ', j)
    return ready_jobs


def scan_for_complete():
    """
    Demo routine for getting a list of complete jobs.  This will be replaced
    by a routine implementing the civet_status completeness tests.
    For this demo, we simply mark all the submitted jobs as complete.
    :return: A list of complete jobs.
    """
    submitted_status_id = get_status_id('Submitted')
    complete_status_id = get_status_id('Complete')
    submitted_jobs = session.query(Job).filter_by(
        status_id=submitted_status_id).all()
    print('\nSubmitted jobs which have just completed:')
    for job in submitted_jobs:
        job.status_id = complete_status_id
        print('   ', job)
    session.commit()
    return submitted_jobs


def init_statuses():
    """
    Create database records for the statuses we need to track.  This is real
    code that can probably go into the final solution.
    :return: None
    """
    # Create the statuses
    statuses = ['Not Submitted', 'Submitted', 'Complete', 'Failed']
    print("Creating four statuses.  They are:", statuses)

    for status in statuses:
        session.add(Status(status))
    session.commit()


def get_status_id(name):
    """
    Throughout, we need to set and query on various statuses.  We need the ID
    associated with a status name.
    :param name: The name of the status.
    :return: The id associated with the name
    """
    id = session.query(Status.id).filter(Status.name == name).one()[0]
    return id


def create_demo_jobs():
    """
    Create four demonstration job records.  This is for demo purposes only.
    :return: None
    """
    # Two "start" jobs with no dependencies
    unsubmitted_status_id = \
        session.query(Status.id).filter(Status.name == 'Not Submitted').one()[0]
    print('\nNow creating four jobs.  Job 3 depends on jobs 1 and 2, and job 4 depends on job 1 and job 3.')
    j1 = Job('Path to script 1', unsubmitted_status_id, [])
    j2 = Job('Path to script 2', unsubmitted_status_id, [])

    # Have to put these into the session and commit them to get their IDs.
    session.add(j1)
    session.add(j2)
    session.commit()

    # Now two dependent jobs
    j3 = Job('Path to script 3', unsubmitted_status_id, [j1, j2])
    session.add(j3)
    session.commit()

    j4 = Job('Path to script 4', unsubmitted_status_id, [j1, j3])

    session.add(j4)
    session.commit()

    session.commit()


def get_all_jobs():
    jobs = session.query(Job).all()
    return jobs

def all_complete():
    print('TODO: MUST IMPLEMENT all_complete()')
    return True


def main_loop():
    while True:
        runnable = scan_for_runnable_jobs()
        if not runnable and all_complete():
            # We're done!
            break
        for job in runnable:
            mark_submitted(job, demo_job_prefix + str(job.id))

        time.sleep(1)

        complete = scan_for_complete()
        for job in complete:
            mark_complete_and_release_dependencies(
                demo_job_prefix + str(job.id))

        all_jobs = get_all_jobs()
        print("The current status of all jobs is:")
        for job in all_jobs:
            print(job)

def main():
    print("Creating status records")
    init_statuses()
    print("Retrieving statuses from DB")
    # Now get the statuses back out of the DB.
    result = session.query(Status)
    for row in result:
        print(row)
    print()

    print("\nCreating demo jobs")
    create_demo_jobs()
    all_jobs = get_all_jobs()

    for j in all_jobs:
        print(j)

    print('\nEntering main_loop')
    main_loop()
    print("Done!")


if __name__ == '__main__':
    # Start things up
    # For this early experimentation we don't use a file.
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine, checkfirst=True)
    Session = sessionmaker(bind=engine)
    session = Session()
    demo_job_prefix = 'cadillac:'

    main()