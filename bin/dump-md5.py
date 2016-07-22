#!/usr/bin/env python
# -*- coding: utf-8 -*-

from xml.sax.handler import ContentHandler
from  xml.sax._exceptions import SAXParseException
from xml.sax import make_parser
import argparse
import zipfile
import sys

class DocumentHandler(ContentHandler):
    def __init__(self, origin_file):
        self.origin_file = origin_file
        self.md5 = ''
        self.filename = ''
        self.inmd5 = False
        self.inFileName = False
        # Init database

    def startElement(self, name, attrs):
        if name == "md5":
            self.inmd5 = True
            self.md5 = ''
        if name == "filename":
            self.inFileName = True
            self.filename = ''

    def endElement(self, name):
        if name == "filename":
            self.inFileName = False
        if name == "md5":
            self.inmd5 = False
        if self.md5 and self.filename:
            print self.md5, self.filename
            self.md5 = '' 
            self.filename = ''

    def characters(self, content):
        if self.inmd5:
            self.md5 += content
        if self.inFileName:
            self.filename += content

    def terminate(self):
        pass

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Parse an XML whitelist file.')
    parser.add_argument("-f", "--file", required=True, help="File to parse.")
    args = parser.parse_args()

    document = DocumentHandler(args.file)
    saxparser = make_parser()
    saxparser.setContentHandler(document)

    if zipfile.is_zipfile(args.file):
        with zipfile.ZipFile(args.file, 'r') as datasource:
            for name in datasource.namelist():
                with datasource.open(name) as content:
                    try:
                        saxparser.parse(content)
                    except SAXParseException,e:
                        sys.stderr.write("Failed to parse "+name + " in " +args.file + "\n")
    else:
        with open(args.file, "r") as datasource:
            saxparser.parse(datasource)
    document.terminate()
