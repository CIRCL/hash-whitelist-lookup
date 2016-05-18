#!/usr/bin/python
import sys
from xml.sax.handler import ContentHandler
from xml.sax import make_parser
import sys
import redis
from unidecode import *

class DocumentHandler(ContentHandler):
    def __init__(self, server, host):
        self.buf = []
        self.inSha256 = False
        self.inFileName = False
        self.pipelinesize = 100
        self.hashes = []
        self.nbuf = []
        self.currenthash = ""
        self.currentfilename = ""
        #Init database
        self.red = redis.Redis(host="127.0.0.1", port=8323, db=0)
        self.pipe = self.red.pipeline()


    def startElement(self, name, attrs):
        if name == "sha256":
            self.inSha256 = True
        if name == "filename":
            self.inFileName = True

    def endElement(self,name):
        if name == "filename":
            self.inFileName = False
            self.currentfilename = "".join(self.nbuf)
            self.nbuf = []

        if name == "sha256":
            self.inSha256 = False
            self.currenthash =  "".join(self.buf)
            self.buf = []

        if len(self.currenthash) > 0 and len(self.currentfilename) > 0:
            if len(self.hashes) == self.pipelinesize:
                #Put hashes in leveldb
                for (h,f) in self.hashes:
                    #FIXME python leveldb python wrapper does not handle unicode values correctly
                    self.pipe.set(h,f)
                self.pipe.execute()
                self.hashes = []
            else:
                self.hashes.append((self.currenthash, self.currentfilename))
                self.currenthash = ""
                self.currentfilename = ""

    def  characters(self,content):
        if self.inSha256:
            self.buf.append(content)
        if self.inFileName:
            self.nbuf.append(content)

    def terminate(self):
        self.pipe.execute()

document = DocumentHandler("127.0.0.1", 8323)
saxparser = make_parser()
saxparser.setContentHandler(document)

filename=sys.argv[1]
print "#Processing filename",filename
datasource = open(filename,"r")
saxparser.parse(datasource)

document.terminate()

