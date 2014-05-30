#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Create a simple filelist out of wikiExtractor.py produced files
Run Stanford Split & Tokenize
Extract and save tokens/sentence
Author: Byron Georgantopoulos (byron@admin.grnet.gr)
"""

import os, sys
import codecs
import re

stanford_basedir = 'stanford-corenlp-full-2014-01-04'
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

# Create and sort the whole list
flist = sorted(readDir(os.walk(sys.argv[1])))
flistsize = len(flist)
run = "mkdir -p "+stanford_basedir+"/TOK"	#; rm -f XML/*"
os.system(run)

# group files by 'step' (process and delete) to avoid disk getting full
i = 0
step = 100
while i<flistsize:
    # create filelist for 'step' files
    flistfp = open(stanford_basedir+'/filelist', 'w')
    for cnt in xrange(i, min(i+step, flistsize)):
        flistfp.write(flist[cnt]+"\n")
    flistfp.close()
    result = os.system(run)

    # script to run stanford nlp splitting and tokenization over a filelist (and thus reduce startup time)
    run="cd "+stanford_basedir+"; java -cp stanford-corenlp-3.3.1.jar:stanford-corenlp-3.3.1-models.jar:xom.jar:joda-time.jar:jollyday.jar:ejml-0.23.jar -Xmx4g edu.stanford.nlp.pipeline.StanfordCoreNLP -annotators tokenize,ssplit -tokenize.options untokenizable=noneDelete,normalizeParentheses=false,normalizeOtherBrackets=false,latexQuotes=false -outputFormat text -filelist filelist -outputDirectory TOK"
    print run
    result = os.system(run)

    # post-process the tokenized output to remove markup
    for cnt in xrange(i, min(i+step, flistsize)):
        print "Processing file "+flist[cnt]+'.out'
        tokfp = open(stanford_basedir+'/TOK/'+os.path.split(flist[cnt])[1]+'.out', 'r')
        txtfp = open(stanford_basedir+'/TOK/'+os.path.split(flist[cnt])[1]+'.tok', 'w')

        line = codecs.getreader("utf-8")(tokfp.readline())
        try:
            while line:
                line = line[:-1]
	        if line[0:6] == '[Text=':	# this is a line that contains markup - ignore all other lines
                   line = re.sub(r'\[Text=([^ ]+) CharacterOffsetBegin=[0-9]+ CharacterOffsetEnd=[0-9]+\]', r'\1', line)
                   line = re.sub(r' \. $', r'', line)                
                   txtfp.write(line+'\n')
                line = codecs.getreader("utf-8")(tokfp.readline())
        except "end of file":
            tokfp.close()
            txtfp.close()

        # delete tokenized output file to save disk space
        run = "rm -f "+stanford_basedir+'/TOK/'+os.path.split(flist[cnt])[1]+'.out'
        result = os.system(run)
    i = i+step
    if i>flistsize:
        break

