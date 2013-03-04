#! /usr/bin/env python
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
class FileInfo():
    #
    # A set of "gold" file info, read in from disk.
    #
    validatedFiles = None
    
    # The set of files found on the current system
    files = []

    # Determine info about a file named fn
    def __init__(self, fn):
    
        # Take care of path and links
        self.name = FileInfo.expandName(fn)

        # Now that we have our best version of the filename, does it exist?
        if not os.path.exists(self.name):
            raise Exception('No such file: ' + self.name)

        # Get the stat info
        s = os.stat(self.name)
        self.mode = s.st_mode
        self.modeString = oct(s.st_mode)
        self.uid = s.st_uid
        self.gid = s.st_gid
        self.size = s.st_size
        self.mtime = s.st_mtime

        self.getHash()

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
        for n in range(len(parts)):
            # One dll at a time. Prune the address from the right.
            # If there is a path, it is the second element, use it.
            # If that's empty, use the first element.
            names = parts[n].split(' (')[0]
            names = names.split('=>')
            if len(names) > 1:
                name = names[1].strip()
            else:
                name = ''
            if name == '':
                name = names[0].strip()
            parts[n] = name
            
        return parts


"""
    A class to track the details of many files, encapsulated in FileInfo
    objects.
"""
class FileCollection(object):

    def __init__(self):
        self.fileList = []
        self.fileDict = {}

    def addFile(self, fn):
        nextStartingPoint = len(self.fileList)
        # Normalize the filename.
        fn = FileInfo.expandName(fn)

        # If we've already explored this file, nothing to do.
        if fn in self.fileList:
            return

        f = FileInfo(fn)
        self.fileDict[fn] = f
        self.fileList.append(f)
        
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
                    if dll not in self.fileList:
                        foundOne = True
                        f = FileInfo(dll)
                        self.fileDict[dll] = f
                        self.fileList.append(f)

    def toJSON(self):
        self.fileDict = {}
        for f in self.fileList:
            self.fileDict[f.name] = f
        return json.dumps(self.fileDict, cls=FileCollection.MyEncoder, indent=2)

    def toJSONFile(self, fn):
        with open(fn, 'w') as f:
            f.write(self.toJSON())

    def fromJSONFile(self, fn):
        with open(fn) as f:
            self.fileDict = json.load(f)

    class MyEncoder(json.JSONEncoder):
        def default(self, obj):
            if not isinstance(obj, FileInfo):
                return super(MyEncoder, self).default(obj)

            return obj.__dict__

def main():
    a = FileCollection()
    a.addFile('bash')
    a.addFile('gcc')
    a.addFile('awk')
    a.addFile('python')
    a.addFile('ruby')
    a.addFile(sys.argv[0])

    print 'Data as parsed.'
    print a.toJSON()
    a.toJSONFile('junk.json')
    
    b = FileCollection()
    b.fromJSONFile('junk.json')
    print "\n\nAfter processing through JSON file..."
    print b.toJSON()
    
    
if __name__ == '__main__':
    main()