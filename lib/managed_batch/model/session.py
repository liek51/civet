"""
This class is how we communicate the SQLAlchemy session amongst various model
files.  The session member's value is set during SQLAlchemy's startup, and
then evermore available here.  If you need it, just import this file.
"""
class Session(object):
    session = None

    # A series of convenience methods, to avoid having to type
    # "Session.session..." everywhere.
    @staticmethod
    def add(domain_object):
        Session.session.add(domain_object)

    @staticmethod
    def commit():
        Session.session.commit()

    @staticmethod
    def query(domain_object):
        return Session.session.query(domain_object)