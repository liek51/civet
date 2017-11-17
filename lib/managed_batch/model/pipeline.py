# Copyright 2017 The Jackson Laboratory
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

"""
Domain object for tracking Civet jobs in the incremental submission system
"""
from sqlalchemy import *
from base import Base
from job import Job
from session import Session
from status import Status
import logging

from sqlalchemy.orm import relationship

class Pipeline(Base):
    __tablename__ = 'pipeline'

    id = Column(Integer, primary_key=True, autoincrement=True)
    status_id = Column(Integer, default=Status.NOT_SET, nullable=False)
    name = Column(String(50), nullable=False)
    log_directory = Column(String(512), nullable=False)
    jobs = relationship('Job', back_populates='pipeline')

    def __init__(self, name, log_directory):
        """
        Create a new pipeline.  All pipelines are created in Status "Not
        Submitted."
        :param name: The name of the pipeline.
        :param log_directory: The log directory for the pipeline. Unlike
            all the other paths in Civet, there is only one log directory per
            pipeline.
        """
        self.name = name
        self.log_directory = log_directory
        self.status_id = Status.NOT_SUBMITTED
        logging.debug("Created pipeline for {} in {}".format(name, log_directory))

    def is_complete(self):
        """
        Check whether all the jobs are complete.  If so, mark the pipeline as
        complete.  If any of the jobs have failed,  mark it as "Failed"
        :return: Boolean
        """

        # if we already are marked as Complete, Failed, or Deleted return
        if self.status_id in [Status.FAILED, Status.COMPLETE, Status.DELETED]:
            return True

        # status

        incomplete_jobs = Session.query(Job). \
            filter(Job.status_id != Status.COMPLETE). \
            filter(Job.pipeline_id == self.id).all()

        if not incomplete_jobs:
            self.status_id = Status.COMPLETE
            Session.commit()
            return True

        # If any are complete, then we have submitted the pipeline.

        any_submitted = len(incomplete_jobs) < len(self.jobs)
        any_failed = False

        for job in incomplete_jobs:
            if job.is_status('Failed'):
                logging.debug("Job {} failed, marking pipeline {} (log dir: {}) failed.".format(job.job_name, self.name, self.log_directory))
                self.status_id = Status.FAILED
                Session.commit()
                any_failed = True
                break

            if job.is_status('Submitted') or job.is_status('Complete'):
                # If anything is submitted or already complete, but
                # we're not all complete, then the pipeline is submitted.
                any_submitted = True

        if any_failed:
            for job in self.jobs:
                if job.status_id == Status.NOT_SUBMITTED:
                    logging.debug("Marking job {} (log dir: {}) as 'failed pipeline'.".format(job.job_name, self.log_directory))
                    job.status_id = Status.PIPELINE_FAILURE
                    Session.commit()
            # A failed pipeline is still considered complete
            return True

        if any_submitted and self.status_id != Status.SUBMITTED:
            logging.debug("Marking pipeline {} (log dir: {}) submitted.".format(self.name, self.log_directory))
            self.status_id = Status.SUBMITTED
            Session.commit()

        # But if we get here, we're not complete.
        return False

    def is_status(self, name):
        return self.status_id == Status.get_id(name)

    def get_status(self):
        return Status.get_name(self.status_id)

    def __repr__(self):
        return '<Pipeline: ID={0} Name={1} Status={2} LogDirectory={3} ' \
               'Jobs={4}>'.format(
                    self.id,
                    self.name,
                    self.status_id,
                    self.log_directory,
                    self.jobs
                )

    def __str__(self):
        """
        This is a duplicate of __repr__().  We can't just call it, because
        it in turns calls the formatting methods in Job.  If that is done from
        __repr__(), it calls Job.__repr__().  We want it to call Job.__str__().
        :return: A formatted representation of the Pipeline and all contained
            jobs.
        """
        jobs = []
        for job in self.jobs:
            jobs.append(job.str_for_pipeline())
        status_name = Status.get_name(self.status_id)
        return '<Pipeline: ID={0} Name="{1}" Status="{2}" ' \
               'LogDirectory="{3}" Jobs={4}>'.format(
            self.id,
            self.name,
            status_name,
            self.log_directory,
            jobs
        )
