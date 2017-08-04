#! /usr/bin/env python

from sqlalchemy import create_engine
from sqlalchemy.orm import relationship, backref, sessionmaker

from base import Base


from job import Job
from status import Status

# Start things up
# For this early experimentation we don't use a file.
engine = create_engine("sqlite:///:memory:")
Base.metadata.create_all(engine, checkfirst=True)
Session = sessionmaker(bind=engine)
session = Session()

# Create the statuses

unsub = Status('Not Submitted')
elig = Status('Eligible')
subm = Status('Submitted')
complete = Status('Complete')
failed = Status('Failed')

session.add(unsub)
session.add(elig)
session.add(subm)
session.add(complete)
session.add(failed)

session.commit()

# Now get the statuses back out of the DB.
# And create a name -> statusID
result = session.query(Status)
for row in result:
    print(row)

# Create a bunch of jobs

# Two "start" jobs with no dependencies
j1 = Job('Path to script 1', elig.id, [])
j2 = Job('Path to script 2', elig.id, [])

# Have to put these into the session and commit it to get their IDs.
session.add(j1)
session.add(j2)
session.commit()

# Now two dependent jobs
j3 = Job('Path to script 3', unsub.id, [j1, j2])
j4 = Job('Path to script 4', unsub.id, [j2])

session.add(j3)
session.add(j4)
session.commit()

for j in [j1, j2, j3, j4]:
    print(j)
