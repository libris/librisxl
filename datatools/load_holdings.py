#!/usr/bin/env python

import xml.etree.ElementTree as ET
import urllib2, base64
import subprocess, os, sys
import argparse

JAVA_CLASSPATH="%s/../whelk-core/classes/main/:../whelk-core/build/libs/whelk-core.jar" % os.path.dirname(os.path.realpath(__file__))
MARC_CONVERTER="se.kb.libris.conch.converter.MarcJSONConverter"
HOLD_DIR="%s/../whelk-core/src/test/resources/marc2jsonld/in/hold" % os.path.dirname(os.path.realpath(__file__))

parser = argparse.ArgumentParser(description='Load holdings from OAI-PMH for a given bibid')
parser.add_argument('-b', '--bibid')
parser.add_argument('-u', '--username', help='The username to use for OAI-PMH')
parser.add_argument('-p', '--password', help='The password to use for OAI-PMH')

args = parser.parse_args()

try:
    os.makedirs(HOLD_DIR)
except:
    1

request = urllib2.Request("http://data.libris.kb.se/hold/oaipmh?verb=ListRecords&metadataPrefix=marcxml&set=bibid:%s" % args.bibid)
base64string = base64.encodestring('%s:%s' % (args.username,args.password)).replace('\n', '')
request.add_header("Authorization", "Basic %s" % base64string)
result = urllib2.urlopen(request)

xml = ET.fromstring(result.read())

for r in xml.iter('{http://www.openarchives.org/OAI/2.0/}record'):
    identifier = r.find("{http://www.openarchives.org/OAI/2.0/}header/{http://www.openarchives.org/OAI/2.0/}identifier").text.split("/")[-1]
    record = r.find("{http://www.openarchives.org/OAI/2.0/}metadata/{http://www.loc.gov/MARC21/slim}record")
    if (record is not None):
        ET.register_namespace('', 'http://www.loc.gov/MARC21/slim')
        newxml = ET.ElementTree(record)
        print "Converting holding for %s ..." % identifier
        newxml.write("/tmp/hold-%s.xml" % identifier, encoding='UTF-8')
        output,error = subprocess.Popen(["java", "-cp", JAVA_CLASSPATH, MARC_CONVERTER, "-xml", "/tmp/hold-%s.xml" % identifier],stdout=subprocess.PIPE,stderr=subprocess.PIPE).communicate()
        f = open("%s/%s.json" % (HOLD_DIR, identifier), 'w')
        f.write(output)

