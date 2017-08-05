"""
Domain object for tracking Civet jobs in the incremental submission system
"""
from sqlalchemy import *
from base import Base
from sqlalchemy.orm import relationship

dependencies = Table('dependencies', Base.metadata,
                     Column('job_id', Integer, ForeignKey('job.id'),
                            primary_key=True),
                     Column('depends_on', Integer, ForeignKey('job.id'),
                            primary_key=True)
                     )


class Job(Base):
    __tablename__ = 'job'

    id = Column(Integer, primary_key=True, autoincrement=True)
    status_id = Column(Integer, ForeignKey('statuses.id'), nullable=False)
    torque_id = Column(String(30))
    # The script path may be long, but we don't really care as long as we
    # are using sqlite3.  It only has one (unlimites) text type.
    script_path = Column(String(512), nullable=False)
    dependencies = relationship('Job', secondary=dependencies,
                                primaryjoin=id == dependencies.c.job_id,
                                secondaryjoin=id == dependencies.c.depends_on,
                                backref='deps')

    def __init__(self, script_path, status_id, dependencies):
        """
        Create a new Job object.  Most fields are initialized to
        note that we can't record dependencies at this po
        :param script_path: The path to the script to be submitted.
        :param dependencies: List of job ids that this job depends on.
            Must already exist and be committed to the database.
        """
        self.script_path = script_path
        self.status_id = status_id
        self.dependencies = dependencies

    def __repr__(self):
        return '<Job: ID={0} StatusID={1} ScriptPath={2} TorqueID={3} ' \
               'Dependencies={4}>'.format(
                self.id, self.status_id, self.script_path, self.torque_id,
                self.dependencies)


"""
The intended usage for creating additional dependencies, should we ever
need to do that, is:

def record_dependency(job_id, depends_on_id):
    job = session.query(Job).get(job_id)
    dependency  = DBSession.query(Job).get(depends_on_id)
    job.dependencies.append(dependency)

"""
