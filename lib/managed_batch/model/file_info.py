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