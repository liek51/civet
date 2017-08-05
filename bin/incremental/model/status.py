"""
Domain object for tracking Civet jobs in the incremental submission system
"""
from sqlalchemy import *
from base import Base


"""
Statuses that we care about:
 - not submitted (dependencies not met)
 - eligible (eligible for submission)
 - submitted
 - complete
 - failed
"""


class Status(Base):
    __tablename__ = 'statuses'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(30))

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return '<Status: ID={0} Name={1}>'.format(self.id, self.name)