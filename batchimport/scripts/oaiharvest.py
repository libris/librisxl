#!/usr/local/bin/python

import sys
import urllib
import re

if len(sys.argv) != 3:
	print 'usage:', sys.argv[0], '<url> <output mask>'
	exit()

base = sys.argv[1].split('?')[0]
parameters = sys.argv[1].split('?')[1].split('&')

for param in parameters:
	key = param.split('=')[0]

	if key == 'verb':
		verb = param.split('=')[1]


n = 1
print sys.argv[1]
data = urllib.urlopen(sys.argv[1]).read()
f = open(sys.argv[2] + '-' + str(n), 'w')

print >> f, data 
f.close()

p = re.compile(r'>([^<]*)</resumptionToken>', re.MULTILINE)

tokens = p.findall(data)

while len(tokens) != 0:
	n = n + 1
	print base + '?verb=' + verb + '&resumptionToken=' + tokens[0].strip()
	data = urllib.urlopen(base + '?verb=' + verb + '&resumptionToken=' + tokens[0].strip()).read()
	f = open(sys.argv[2] + '-' + str(n), 'w')
	print >> f, data
	f.close()

	tokens = p.findall(data)
