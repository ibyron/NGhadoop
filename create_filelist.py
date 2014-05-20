#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Create a simple filelist out of wikiExtractor.py produced files
Author: Byron Georgantopoulos (byron@admin.grnet.gr)
"""

import os, sys


"""
Recursively read a directory and create filenames list
"""
def readDir(dir):
    flist = []

    for dirname, dirnames, filenames in dir:
        for filename in filenames:
            flist.append(os.path.join(dirname, filename))
    return flist

if len(sys.argv) != 2:
    print "Incorrect number of arguments"
    sys.exit(-1)


flist = sorted(readDir(os.walk(sys.argv[1])))

for filename in flist:
    print filename


