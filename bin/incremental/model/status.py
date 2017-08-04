# IMport SQLAlchemy stuff here.
# This is a sketch; I haven't looked up the actual syntax for the details.

"""
Statuses that we care about:
 - not submitted (dependencies not met)
 - eligible (eligible for submission)
 - submitted
 - complete
 - failed
"""
class Status(Base):
    id = Column(primary, integer)
    name = Column(text(30))