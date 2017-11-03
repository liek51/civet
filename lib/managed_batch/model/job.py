"""
Domain object for tracking Civet jobs in the incremental submission system
"""
from sqlalchemy import *
from base import Base
import logging
from sqlalchemy.orm import relationship
from sqlalchemy import func
from session import Session
from status import Status

dependencies = Table('job_dependencies', Base.metadata,
                     Column('job_id', Integer, ForeignKey('job.id'),
                            primary_key=True),
                     Column('depends_on', Integer, ForeignKey('job.id'),
                            primary_key=True)
                     )


class Job(Base):
    __tablename__ = 'job'

    id = Column(Integer, primary_key=True, autoincrement=True)
    pipeline_id = Column(Integer, ForeignKey('pipeline.id'))
    pipeline = relationship('Pipeline', back_populates='jobs')
    job_name = Column(String(50), nullable=False)
    threads = Column(Integer, nullable=False)
    # These paths may be long, but we don't really care as long as
    # we are using sqlite3.  It only has one (unlimited) text type.
    stdout_path = Column(String(512), nullable=False)
    stderr_path = Column(String(512), nullable=False)
    script_path = Column(String(512), nullable=False)
    epilog_path = Column(String(512), nullable=False)
    walltime = Column(String(16))
    mem = Column(Integer)  # Nullable
    email_list = Column(String(512))  # Nullable
    mail_options = Column(String(64))  # Nullable
    queue = Column(String(128))  # Nullable
    status_id = Column(Integer, default=Status.NOT_SET, nullable=False)
    torque_id = Column(String(512))
    env = Column(String(512))
    depends_on = relationship('Job', secondary=dependencies,
                              primaryjoin=id == dependencies.c.job_id,
                              secondaryjoin=id == dependencies.c.depends_on,
                              backref='job_dependencies')

    def __init__(self, pipeline, job_name, threads, stdout_path,
                 stderr_path, script_path, epilog_path, mem,
                 email_list, mail_options, env, depends_on, queue, walltime):
        """
        Create a new Job object.  All jobs are created in state Not Submitted.
        :param pipeline: The pipeline of which this job is a part.
        :param job_name: The name of this job.
        :param threads: Number of threads to allocate.
        :param stdout_path: Stdout path for torque (not the commands)
        :param stderr_path: Stderr path for torque (not the commands)
        :param script_path: The path to the script to be submitted.
        :param epilog_path: Path to the epilog script.
        :param mem: Amount of mem to allocate for this job. None means
            unlimited.
        :param email_list: Email address(es) to send status mail to.
        :param mail_options: Options controlling when email is sent.
        :param env: Environment to set for the running job.
        :param depends_on: List of job ids that this job depends on.
            Must already exist and be committed to the database.
        """
        self.pipeline = pipeline
        self.job_name = job_name
        self.threads = threads
        self.stdout_path = stdout_path
        self.stderr_path = stderr_path
        self.script_path = script_path
        self.epilog_path = epilog_path
        self.mem = mem
        self.email_list = email_list
        self.mail_options = mail_options
        self.env = env
        self.status_id = Status.get_id("Not Submitted")
        self.depends_on = depends_on
        self.queue = queue
        self.walltime = walltime

    def is_status(self, name):
        return self.status_id == Status.get_id(name)

    def set_status(self, name):
        self.status_id = Status.get_id(name)

    def get_status(self):
        return Status.get_name(self.status_id)

    def mark_submitted(self, torque_id):
        logging.debug('Marked submitted: {} (log dir: {})'.format(
            self.job_name, self.pipeline.log_directory))
        self.status_id = Status.SUBMITTED
        self.torque_id = torque_id
        Session.commit()

    def mark_complete_and_release_dependencies(self):
        # Now let's complete that job.
        logging.debug('Now completing job: {}, ID: {} (log dir: {})'.format(
            self.job_name, self.id, self.pipeline.log_directory))
        self.status_id = Status.COMPLETE

        # Find all the jobs depending on the completed job.
        dependent_jobs = Session.query(Job).filter(
            Job.depends_on.any(Job.id == self.id))
        for j in dependent_jobs:
            logging.debug('Found dependent job: {0}'.format(j.job_name))
            j.depends_on.remove(self)
            logging.debug("New dependencies with completed job removed: {0}".
                          format([x.job_name for x in j.depends_on]))
        Session.commit()

    def __repr__(self):
        return '<Job: ID={0} Pipeline={1} JobName={2} ' \
               'StdoutPath={3} StderrPath={4} ScriptPath={5} EpilogPath={6} ' \
               'Mem={7} EmailList={8} MailOptions={9} Env={10} StatusID={11} ' \
               'TorqueID={12} Dependencies={13} Queue={14} Walltime={15}>'.\
            format(
                self.id,
                self.pipeline.name,
                self.job_name,
                self.stdout_path,
                self.stderr_path,
                self.script_path,
                self.epilog_path,
                self.mem,
                self.email_list,
                self.mail_options,
                self.env,
                self.status_id,
                self.torque_id,
                self.depends_on,
                self.queue,
                self.walltime)

    def str_for_pipeline(self):
        status_name = Status.get_name(self.status_id)
        return '<Job: ID={0} Name={1} Status={2}>'.format(
            self.id, self.job_name, status_name)

    def __str__(self):
        deps = []
        for dep in self.depends_on:
            deps.append('<Job: ID={0} Name={1}>'.format(dep.id, dep.job_name))

        stat = Status.get_name(self.status_id)

        return '<Job: ID={0} Pipeline="{1}" JobName="{2}" ' \
               'StdoutPath="{3}" StderrPath="{4}" ScriptPath="{5}" ' \
               'EpilogPath="{6}" Mem={7} EmailList="{8}" MailOptions="{9}" ' \
               'Env="{10}" StatusID={11} ' \
               'TorqueID="{12}" Dependencies={13} Queue={14} Walltime={15}>'.\
            format(
                self.id,
                self.pipeline.name,
                self.job_name,
                self.stdout_path,
                self.stderr_path,
                self.script_path,
                self.epilog_path,
                self.mem,
                self.email_list,
                self.mail_options,
                self.env,
                stat,
                self.torque_id,
                deps,
                self.queue,
                self.walltime)

    @staticmethod
    def scan_for_runnable(limit=None):
        """
        Scans the database for jobs that are eligible to run; in other words,
        those with an empty dependency list and the status "Not Submitted".
        :param limit: Place an arbitrary limit on the number of jobs returned.
        :return: A list of runnable jobs.
        """
        logging.debug('Finding runnable jobs')
        ready_jobs_query = Session.query(Job).filter(~Job.depends_on.any()). \
            filter_by(status_id=Status.NOT_SUBMITTED)
        if limit:
            ready_jobs_query = ready_jobs_query.limit(limit)
        ready_jobs = ready_jobs_query.all()
        if not ready_jobs:
            logging.debug('No jobs are ready to execute')
        else:
            logging.debug('The jobs that are ready to execute are:')
            for j in ready_jobs:
                logging.debug('    {} (log dir: {})'.format(
                    j.job_name, j.pipeline.log_directory))
        return ready_jobs

    @staticmethod
    def count_submitted():
        count_q = Session.query(Job).filter_by(
            status_id=Status.SUBMITTED).statement. \
            with_only_columns([func.count()]).order_by(None)
        count = Session.session.execute(count_q).scalar()
        logging.debug("Counted {} submitted jobs".format(count))
        return count

    @staticmethod
    def get_all():
        jobs = Session.query(Job).all()
        logging.debug("Jobs.get_all() returned {} jobs".format(len(jobs)))
        return jobs
