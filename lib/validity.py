#! /usr/bin/env python

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

# validity.py: A library of routines to test the "validity" of files that are
# supposed to be locked down.
#

from __future__ import print_function

import sys
import hashlib
import binascii
import os
import commands
import json
import stat


# A class to track interesting information about files.
class FileInfo(object):
    #
    # A set of "gold" file info, read in from disk.
    #
    validatedFiles = None
    
    # The set of files found on the current system
    files = []

    def __init__(self):
        self.name = None
        self.compare_failures = None
        self.mode = None
        self.mode_string = None
        self.uid = None
        self.gid = None
        self.size = None
        self.sha1 = None
        
    def cache_attrs(self, fn):
        """
        Determine the info we need to validate the file named fn.
        :param fn: The name of the file to validate.
        :return: None
        """
        # Take care of path and links
        self.name = FileInfo.expand_name(fn)

        # Now that we have our best version of the filename, does it exist?
        if not os.path.exists(self.name):
            self.mode = 'No such file.'
            return

        # Get the stat info
        s = os.stat(self.name)
        self.mode = s.st_mode
        self.mode_string = oct(s.st_mode)
        self.uid = s.st_uid
        self.gid = s.st_gid
        self.size = s.st_size

        self.get_hash()

    def reload(self, dct):
        """
        When we wre updating a file, stash info from the existing dict.
        :param dct: A dictionary of existing file information.
        :return: None
        """
        self.name = dct['name']
        self.mode = dct['mode']
        if self.mode == 'No such file.':
            return

        # The following attributes don't exist if the file was not found.
        self.sha1 = dct['sha1']
        self.uid = dct['uid']
        self.gid = dct['gid']
        self.mode_string = dct['mode_string']
        self.size = dct['size']

    @staticmethod
    def expand_name(fn):
        """
        Many of the files we'll be testing are executables, and therefore
        looked up on the path.  Try to look it up there (use the shell).
        Note: This uses a deprecated interface to scan the PATH variable,
        but boy is it handy.
        If the returned status is non-zero, the lookup using the 'which' command
        did not succeed, so we continue processing with the passed-in name.
        :param fn: The file name to expand to a real path.
        :return: The realpath of the passed in filename.
        """
        (status, result) = commands.getstatusoutput('which ' + fn)
        if status == 0:
            fn = result

        # The file may be a link, or the path may contain one...
        return os.path.realpath(fn)

    def effectively_readable(self):
        """
        Find out if the file will be readable even with possible setgid or
        setuid.  Optimized for the usual case of no setgid or setuic.
        :return: True if the file is readable.
        """
        uid = os.getuid()
        euid = os.geteuid()
        gid = os.getgid()
        egid = os.getegid()

        # This is probably true most of the time, so just let os.access()
        # handle it.  Avoids potential bugs in the rest of this function.
        if uid == euid and gid == egid:
            return os.access(self.name, os.R_OK)

        st = os.stat(self.name)

        # This may be wrong depending on the semantics of your OS.
        # i.e. if the file is -------r--, does the owner have access or not?
        if st.st_uid == euid:
            return st.st_mode & stat.S_IRUSR != 0

        # See comment for UID check above.
        groups = os.getgroups()
        if st.st_gid == egid or st.st_gid in groups:
            return st.st_mode & stat.S_IRGRP != 0

        return st.st_mode & stat.S_IROTH != 0        

    def get_hash(self):
        """
        Compute the sha1 checksum of the file.
        :return: None
        """
        if not self.effectively_readable():
            self.sha1 = 'Not readable'
            return

        blocksize = 65536
        h = hashlib.sha1()
        with open(self.name) as f:
            buf = f.read(blocksize)
            while len(buf) > 0:
                h.update(buf)
                buf = f.read(blocksize)
        self.sha1 = binascii.hexlify(h.digest())

    def get_dlls(self):
        """
        Since we want to validate that the environment hasn't changed, we need
        to add all the DLLs used by this file into the list to be validated
        later on in this run.
        :return: A list of DLLs used by this file, as determined by the ldd
            command.
        """
        if not self.effectively_readable():
            return []
        # Uses the system's ldd command to get all the supporting libraries.
        # Note: This uses a deprecated interface, but boy is it handy.
        (status, result) = commands.getstatusoutput('ldd ' + self.name)
        if status != 0:
            return []
        
        parts = result.split('\n')
        processed = []
        for n in range(len(parts)):
            # One dll at a time. Prune the address from the right.
            # If there is a path, it is the second element, use it.
            # If that's empty, use the first element.
            if 'ldd: warning:' in parts[n]:
                continue
            names = parts[n].split(' (')[0]
            names = names.split('=>')
            if len(names) > 1:
                name = names[1].strip()
            else:
                name = ''
            if name == '':
                name = names[0].strip()
            processed.append(name)
        return processed 

    def update_compare_failures(self, msg, val1, val2):
        if not self.compare_failures:
            self.compare_failures = ''
        self.compare_failures += '    {0} differs.\n        Master: {1}\n' \
                                 '        Found:  {2}\n'.\
            format(msg, val1, val2)

    def __eq__(self, other):
        if not isinstance(other, FileInfo):
            raise Exception('Other instance must be a FileInfo!')
        if self.sha1 != other.sha1:
            self.update_compare_failures('sha1', other.sha1, self.sha1)

        # Special check: If either file doesn't exist, we fail.
        if self.mode == 'No such file.':
            self.update_compare_failures('mode', other.mode, self.mode)

        # Equal if no failures
        return self.compare_failures is None

    def __ne__(self, other):
        return not self.__eq__(other)


class FileCollection(object):
    """
        A class to track the details of many files, encapsulated in FileInfo
        objects.
    """

    def __init__(self):
        self.file_list = []
        self.file_name_list = []
        self.file_dict = {}

    def add_file(self, fn):
        next_starting_point = len(self.file_list)
        # Normalize the filename.
        fn = FileInfo.expand_name(fn)

        # If we've already explored this file, nothing to do.
        if fn in self.file_name_list:
            return

        f = FileInfo()
        f.cache_attrs(fn)
        self.file_dict[fn] = f
        self.file_list.append(f)
        self.file_name_list.append(f.name)
        
        found_one = True
        while found_one:
            found_one = False
            # We want to walk over all the unexplored files in the list.
            starting_point = next_starting_point
            next_starting_point = len(self.file_list)
            for n in range(starting_point, next_starting_point):
                dlls = self.file_list[n].get_dlls()
                for dll in dlls:
                    # We're not interested in recording dependencies for
                    # something that is statically linked. There are none.
                    if dll == 'statically linked':
                        continue
                    # The "shared library" linux-vdso.so.1 isn't real.  It is
                    # Linux magic.
                    if dll == 'linux-vdso.so.1':
                        continue
                    # Normalize the filename (probably not necessary)
                    dll = os.path.realpath(dll)
                    if dll not in self.file_name_list:
                        found_one = True
                        f = FileInfo()
                        f.cache_attrs(dll)
                        self.file_dict[dll] = f
                        self.file_list.append(f)
                        self.file_name_list.append(f.name)

    """
    Add FileInfo instances into self, if they aren't already
    in the list.  Changes existing entries if update=True.
    """
    def merge(self, other, update):
        if not isinstance(other, FileCollection):
            raise Exception("other must be a FileCollection")
        for fn in other.file_dict:
            if (fn not in self.file_dict) or update:
                fi = other.file_dict[fn]
                self.file_dict[fn] = fi

        self.file_list = []
        self.file_name_list = []
        for fn in self.file_dict:
            fi = self.file_dict[fn]
            self.file_list.append(fi)
            self.file_name_list.append(fi.name)

    def to_json(self):
        self.file_dict = {}
        for f in self.file_list:
            self.file_dict[f.name] = f
        return json.dumps(self.file_dict, cls=FileCollection.MyEncoder, indent=2)

    def to_json_file(self, fn):
        with open(fn, 'w') as f:
            f.write(self.to_json())

    def from_json_file(self, fn):
        with open(fn) as f:
            json_data = json.load(f)
        for fn in json_data:
            fi = FileInfo()
            fi.reload(json_data[fn])
            self.file_dict[fn] = fi
            self.file_list.append(fi)
            self.file_name_list.append(fi.name)
            
    def validate(self, master):
        msg = ''
        for fi in self.file_list:
            if fi.name in master.file_dict:
                m = master.file_dict[fi.name]
                if fi != m:
                    msg += 'Validation failure:' + fi.name + '\n'
                    msg += fi.compare_failures
            else:
                msg += 'Validation failure:' + fi.name + '\n'
                msg += '    Not found in the master file list\n'
        return msg

    class MyEncoder(json.JSONEncoder):
        def default(self, obj):
            if not isinstance(obj, FileInfo):
                # PyCharm emits an "unresolved symbol" error for MyEncoder.
                # The code is correct, PyCharm is not.
                return super(MyEncoder, self).default(obj)

            return obj.__dict__


def main():
    """
    A test method.  This module is not intended to be run as a main program.
    :return: None.
    """
    a = FileCollection()
    a.add_file('bash')
    a.add_file('gcc')
    a.add_file('awk')
    a.add_file('python')
    a.add_file('ruby')
    a.add_file(sys.argv[0])

    print('Data as parsed.')
    print(a.to_json())
    a.to_json_file('junk.json')
    
    b = FileCollection()
    b.from_json_file('junk.json')
    print("\n\nAfter processing through JSON file...")
    print(b.to_json())
    
    
if __name__ == '__main__':
    main()
