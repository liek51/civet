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
Enumerations for tracking Civet jobs in the incremental submission system
"""


#
# CAUTION: The class list NAME must be maintained in parallel with the
#    enumeration values.  YOU HAVE BEEN WARNED!
#
class Status(object):
    NOT_SET = 0
    NOT_SUBMITTED = 1
    ELIGIBLE = 2
    SUBMITTED = 3
    COMPLETE = 4
    FAILED = 5
    DELETED = 6
    PIPELINE_FAILURE = 7

    NAME = [
        'Not Set',
        'Not Submitted',
        'Eligible',
        'Submitted',
        'Complete',
        'Failed',
        'Deleted',
        'Pipeline Failure',
    ]

    def __init__(self):
        raise Exception("Do not instantiate the Status class. "
                        "It is an enumeration")

    @staticmethod
    def get_id(name):
        """
        Throughout, we need to set and query on various statuses.  We need the
        ID associated with a status name.
        :param name: The name of the status.
        :return: The id associated with the name
        """
        return Status.NAME.index(name)

    @staticmethod
    def get_name(status_id):
        """
        Given a status ID, get the name.
        :param status_id: the ID of the status
        :return: The associated name.
        """
        return Status.NAME[status_id]
