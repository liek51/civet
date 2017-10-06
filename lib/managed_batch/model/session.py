# Copyright 2016 The Jackson Laboratory
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
    def query(*entities, **kwargs):
        return Session.session.query(*entities, **kwargs)