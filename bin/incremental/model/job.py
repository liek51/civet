"""
Domain object for tracking Civet jobs in the incremental submission system
"""
from sqlalchemy import *
from base import Base
from sqlalchemy.orm import relationship
from status import Status

dependencies = Table('dependencies', Base.metadata,
                     Column('job_id', bigint, ForeignKey('job.id'),
                            name='a', primary_key=True),
                     Column('depends_on', bigint, ForeignKey('job.id'),
                            name='b', primary_key=True)
                     )


class Job(Base):
    __tablename__ = 'job'

    id = Column(bigint, primary_key=True, autoincrement=True)
    status = Column(Integer, ForeignKey('statuses.id'))
    script_path = Column(types.String(80), nullable=False)  # May be long...
    dependencies = relationship(secondary=dependencies,
                                primaryjoin=id == dependencies.c.job_id,
                                secondaryjoin=id == dependencies.c.depends_on,
                                backref='dependencies')

    def __init__(self, script_path, dependencies):
        """
        Create a new Job object.  Most fields are initialized to
        note that we can't record dependencies at this po
        :param script_path: The path to the script to be submitted.
        :param dependencies: List of job ids that this job depends on.
            Must already exist and be committed to the database.
        """
        self.script_path = script_path
        self.status = 

"""
The intended usage for creating dependencies is:

def record_dependency(job_id, depends_on_id):
    job = DBSession.query(Job).get(job_id)
    dependency  = DBSession.query(Job).get(depends_on_id)
    job.dependencies.append(dependency)

"""

