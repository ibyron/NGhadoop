#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Script that parses the xml file, retrieves and saves all tokens (document->sentences->tokens)
Author: Byron Georgantopoulos (byron@admin.grnet.gr)
"""


import os, sys
import lxml
from lxml import etree

tree = etree.parse(sys.argv[1])
root = tree.getroot()
doc = root.find("document")
sents = doc.find("sentences")
for s in sents:
    tokens = s.find("tokens")
    sent_text = ""
    for t in tokens:
        sent_text = sent_text + t.find("word").text.encode("UTF-8") + " "
    print sent_text[:-1]

