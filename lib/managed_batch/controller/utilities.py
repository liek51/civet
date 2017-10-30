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

from __future__ import print_function

import logging

import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from managed_batch.model.base import Base
from managed_batch.model.pipeline import Pipeline

from managed_batch.model.session import Session

__engine = None


def initialize_model(db_path, echo_sql=False):
    """
    Create a connection to a new database.
    Record the session in the Session object.
    :param db_path: The full path to the database to be created.  For testing,
        you can specify :memory:
    :param echo_sql: If true, the SQL will be written to stdout (?err?) as it
        is executed.
    :return: The inintialized session

    NOTE: For some reason I haven't figured out, just setting the
    Session.session seems to set it only for this module and leave it None for
    the rest of the modules.  So we return it, and it is up to our caller to
    set it in the Session class.
    """
    global __engine
    engine = create_engine('sqlite:///{0}'.format(db_path))
    __engine = engine

    # Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine, checkfirst=True)
    session_func = sessionmaker(bind=engine)
    session = session_func()
    engine.echo = echo_sql

    logging.debug("Model initialization is complete.")
    logging.info("Using database {0}".format(
        db_path
    ))
    return session


def dispose_engine():
    __engine.dispose()


def write_batch_id_to_log_dir(batch_id):
    for pipeline in Session.query(Pipeline):
        with open(os.path.join(pipeline.log_directory, "MANAGED_BATCH"),
                  mode='w') as f:
            f.write(batch_id + '\n')
