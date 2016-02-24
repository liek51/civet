#! /usr/bin/env python

"""
Copyright (C) 2016  The Jackson Laboratory

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

# validity.py: A library of routines to test the "validity" of files that are
# supposed to be locked down.
#
import sys
import hashlib
import binascii
import os
import commands
import json

# A class to track interesting information about files.
class FileInfo(object):
    #
    # A set of "gold" file info, read in from disk.
    #
    validatedFiles = None
    
    # The set of files found on the current system
    files = []

    def __init__(self):
        self.compare_failures = None
        
    # Determine info about a file named fn
    def cache_attrs(self, fn):
    
        # Take care of path and links
        self.name = FileInfo.expandName(fn)

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

        self.getHash()

    def reload(self, dct):
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
    def expandName(fn):
        # Many of the files we'll be testing are executables, and therefore
        # looked up on the path.  Try to look it up there (use the shell).
        # Note: This uses a deprecated interface, but boy is it handy.
        # If the returned status is non-zero, the lookup did not succeed.
        (status, result) = commands.getstatusoutput('which ' + fn)
        if status == 0:
            fn = result

        # The file may be a link, or the path may contain one...
        return os.path.realpath(fn)

    def effectivelyReadable(self):
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

    def getHash(self):
        if not self.effectivelyReadable():
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

    def getDLLs(self):
        if not self.effectivelyReadable():
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
        self.compare_failures += '    {0} differs.\n        Master: {1}\n        Found:  {2}\n'.format(
            msg, val1, val2)

    def __eq__(self, other):
        #print >> sys.stderr, type(self), type(other), "REENABLE THROW"
        if not isinstance(other, FileInfo):
            raise Exception('Other instance must be a FileInfo!')
        if self.sha1 != other.sha1:
            self.update_compare_failures('sha1', other.sha1, self.sha1)
        #if self.uid != other.uid:
        #    self.update_compare_failures('uid', other.uid, self.uid)
        #if self.name != other.name:
        #    self.update_compare_failures('name', other.name, self.name)
        #    self.compare_failures += (
        #        '        (This indicates a logic failure in the program. '
        #        'Please report it.)\n')
        #if self.gid != other.gid:
        #    self.update_compare_failures('gid', other.gid, self.gid)
        #if self.mode != other.mode:
        #    self.update_compare_failures('mode', other.mode, self.mode)

        # Special check: If either file doesn't exist, we fail.
        if self.mode == 'No such file.':
            self.update_compare_failures('mode', other.mode, self.mode)

        #if self.mode_string != other.mode_string:
        #    self.update_compare_failures('mode_string', other.mode_string, 
        #                                 self.mode_string)
        #if self.size != other.size:
        #    self.update_compare_failures('size', other.size, self.size)

        # Equal if no failures
        return self.compare_failures is None

    def __ne__(self, other):
        return not self.__eq__(other)

"""
    A class to track the details of many files, encapsulated in FileInfo
    objects.
"""
class FileCollection(object):

    def __init__(self):
        self.fileList = []
        self.fileNameList = []
        self.fileDict = {}

    def add_file(self, fn):
        nextStartingPoint = len(self.fileList)
        # Normalize the filename.
        fn = FileInfo.expandName(fn)

        # If we've already explored this file, nothing to do.
        if fn in self.fileNameList:
            return

        f = FileInfo()
        f.cache_attrs(fn)
        self.fileDict[fn] = f
        self.fileList.append(f)
        self.fileNameList.append(f.name)
        
        foundOne = True
        while foundOne:
            foundOne = False
            # We want to walk over all the unexplored files in the list.
            startingPoint = nextStartingPoint
            nextStartingPoint = len(self.fileList)
            for n in range(startingPoint, nextStartingPoint):
                dlls = self.fileList[n].getDLLs()
                for dll in dlls:
                    # We're not interested in recording dependencies for something
                    # that is statically linked. There are none.
                    if dll == 'statically linked':
                        continue
                    # The "shared library" linux-vdso.so.1 isn't real.  It is Linux magic.
                    if dll == 'linux-vdso.so.1':
                        continue
                    #Normalize the filename (probably not necessary)
                    dll = os.path.realpath(dll)
                    if dll not in self.fileNameList:
                        foundOne = True
                        f = FileInfo()
                        f.cache_attrs(dll)
                        self.fileDict[dll] = f
                        self.fileList.append(f)
                        self.fileNameList.append(f.name)

    """
    Add FileInfo instances into self, if they aren't already
    in the list.  Changes existing entries if update=True.
    """
    def merge(self, other, update):
        if not isinstance(other, FileCollection):
            raise Exception("other must be a FileCollection")
        for fn in other.fileDict:
            if (fn not in self.fileDict) or update:
                fi = other.fileDict[fn]
                self.fileDict[fn] = fi

        self.fileList = []
        self.fileNameList = []
        for fn in self.fileDict:
            fi = self.fileDict[fn]
            self.fileList.append(fi)
            self.fileNameList.append(fi.name)

    def to_JSON(self):
        self.fileDict = {}
        for f in self.fileList:
            self.fileDict[f.name] = f
        return json.dumps(self.fileDict, cls=FileCollection.MyEncoder, indent=2)

    def to_JSON_file(self, fn):
        with open(fn, 'w') as f:
            f.write(self.to_JSON())

    def from_JSON_file(self, fn):
        with open(fn) as f:
            JSON_data = json.load(f)
        for fn in JSON_data:
            fi = FileInfo()
            fi.reload(JSON_data[fn])
            self.fileDict[fn] = fi
            self.fileList.append(fi)
            self.fileNameList.append(fi.name)
            
    def validate(self, master):
        msg = ''
        for fi in self.fileList:
            if fi.name in master.fileDict:
                m = master.fileDict[fi.name]
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
                return super(MyEncoder, self).default(obj)

            return obj.__dict__


def main():
    a = FileCollection()
    a.add_file('bash')
    a.add_file('gcc')
    a.add_file('awk')
    a.add_file('python')
    a.add_file('ruby')
    a.add_file(sys.argv[0])

    print 'Data as parsed.'
    print a.to_JSON()
    a.to_JSON_file('junk.json')
    
    b = FileCollection()
    b.from_JSON_file('junk.json')
    print "\n\nAfter processing through JSON file..."
    print b.to_JSON()
    
    
if __name__ == '__main__':
    main()
