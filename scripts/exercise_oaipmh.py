import requests
from lxml import etree
import time


PMH = "{http://www.openarchives.org/OAI/2.0/}"

def parse_oaipmh(start_url, name, passwd):
    start_time = time.time()
    resumption_token = None
    record_count = 0
    while True:
        url = make_next_url(start_url, resumption_token)
        res = requests.get(url, auth=(name, passwd), stream=True)
        record_root = etree.parse(res.raw)
        record_count += len(record_root.findall("{0}ListRecords/{0}record".format(PMH)))
        resumption_token = record_root.findtext("{0}ListRecords/{0}resumptionToken".format(PMH))
        elapsed = time.time() - start_time
        print "Record count: %s. Got resumption token: %s. Elapsed time: %s. Records/second: %s" % (record_count, resumption_token, elapsed, record_count / elapsed)
        if not resumption_token:
            break

def make_next_url(base_url, resumption_token=None):
    params = "?verb=ListRecords&resumptionToken=%s" % resumption_token if resumption_token else "?verb=ListRecords&metadataPrefix=marcxml"
    return base_url + params

if __name__ == '__main__':
    from sys import argv
    args = argv[1:]
    start_url = (args.pop(0) if len(args) == 3
            else "http://data.libris.kb.se/hold/oaipmh")
    name, passwd = args[:2]
    parse_oaipmh(start_url, name, passwd)
