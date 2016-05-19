#!/usr/bin/env python
# -*- coding: utf-8 -*-

from xml.sax.handler import ContentHandler
from xml.sax import make_parser
import redis
import argparse
import zipfile


class DocumentHandler(ContentHandler):
    def __init__(self, server, host, origin_file):
        self.origin_file = origin_file
        self.sha256 = ''
        self.filename = ''
        self.inSha256 = False
        self.inFileName = False
        # Init database
        red = redis.Redis(host=server, port=host, db=0)
        if red.sismember("FILES", origin_file):
            raise IOError("Skip filename " + origin_file + " as it is already processed")
        self.pipe = red.pipeline()

    def startElement(self, name, attrs):
        if name == "sha256":
            self.inSha256 = True
            self.sha256 = ''
        if name == "filename":
            self.inFileName = True
            self.filename = ''

    def endElement(self, name):
        if name == "filename":
            self.inFileName = False
        if name == "sha256":
            self.inSha256 = False
        if self.sha256 and self.filename:
            # self.pipe.sadd(self.origin_file, self.sha256)  # 14Mb/file (21 files)

            # Test data: 2337800 Hashes
            # self.pipe.setbit(self.sha256, 0, 1)  # 336.01M
            # self.pipe.sadd(self.sha256, self.filename)  # 1.12G
            # self.pipe.sadd(self.sha256[:46], self.sha256[46:])  # 746.15M / 2337800 Keys
            # self.pipe.sadd(self.sha256[:16], self.sha256[16:])  # 746.24M / 2337799 Keys
            # self.pipe.sadd(self.sha256[:8], self.sha256[8:])  # 710.45M / 2337184 Keys
            self.pipe.sadd(self.sha256[:4], self.sha256[4:])  # 296.74M / 118164 Keys

    def characters(self, content):
        if self.inSha256:
            self.sha256 += content
        if self.inFileName:
            self.filename += content

    def terminate(self):
        self.pipe.sadd("FILES", self.origin_file)
        self.pipe.execute()


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Parse an XML whitelist file.')
    parser.add_argument("-f", "--file", required=True, help="File to parse.")
    args = parser.parse_args()

    document = DocumentHandler("127.0.0.1", 8323, args.file)
    saxparser = make_parser()
    saxparser.setContentHandler(document)

    print("#Processing filename", args.file)

    if zipfile.is_zipfile(args.file):
        with zipfile.ZipFile(args.file, 'r') as datasource:
            for name in datasource.namelist():
                with datasource.open(name) as content:
                    saxparser.parse(content)
    else:
        with open(args.file, "r") as datasource:
            saxparser.parse(datasource)
    document.terminate()
