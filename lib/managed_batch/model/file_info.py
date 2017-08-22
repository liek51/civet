

from sqlalchemy import *
from base import Base
import logging
from session import Session



class FileInfo(Base):
    __tablename__ = 'file_info'

    id = Column(Integer, primary_key=True, autoincrement=True)
    schema_version = Column(Integer, nullable=False)
    started = Column(Boolean, nullable=False)

    CURRENT_SCHEMA_VERSION = 1

    def __init__(self, schema_version, started):
        self.schema_version = schema_version
        self.started = started