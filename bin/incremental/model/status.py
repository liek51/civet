"""
Domain object for tracking Civet jobs in the incremental submission system
"""
from sqlalchemy import *
from base import Base
from session import Session


"""
Statuses that we care about:
 - not submitted (dependencies not met)
 - eligible (eligible for submission)
 - submitted
 - complete
 - failed
 - deleted
"""


class Status(Base):
    __tablename__ = 'status'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(30))

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return '<Status: ID={0} Name={1}>'.format(self.id, self.name)

    @staticmethod
    def get_id(name):
        """
        Throughout, we need to set and query on various statuses.  We need the ID
        associated with a status name.
        :param name: The name of the status.
        :return: The id associated with the name
        """
        id = Session.query(Status.id).filter(
            Status.name == name).one()[0]
        return id

    @staticmethod
    def get_name(id):
        """
        Given a status ID, get the name.
        :param id:
        :return:
        """
        status = Session.query(Status).get(id)
        return status.name
