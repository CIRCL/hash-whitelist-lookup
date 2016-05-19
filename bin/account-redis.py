#!/usr/bin/env python
# -*- coding: utf-8 -*-
import redis
import sys
server = "127.0.0.1"
port=8323
nhashes = 0
nkeys = 1 
avg = 0
mn = sys.maxint
mx = 0

red = redis.Redis(host=server, port=port, db=0)
for k in red.keys("*"):
    if k != "FILES":
        n  = red.scard(k)
        if n < mn:
            mn = n
        if n > mx:
            mx = n
        nkeys = nkeys + 1
        nhashes = nhashes + n

avg = float(nhashes) / float(nkeys)
print "*** General ***\n"
print "#Number of keys", nkeys
print "#Number of hashes", nhashes
print "#Average size of sets", avg
print "#Set with the minimal items",mn
print "#Set with the maximal items",mx


print "\n*** Redis info command output ****\n"
data = red.info()
for k in data.keys():
    print k,data[k]
print "\n*** Processed files ****\n"
data = []
for f in red.smembers("FILES"):
    data.append(f)

data.sort()
for f in data:
    print f


