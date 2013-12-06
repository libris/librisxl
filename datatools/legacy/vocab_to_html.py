import sys, os
from rdflib import *
from rdflib.namespace import SKOS
from chameleon import PageTemplate

VANN = Namespace("http://purl.org/vocab/vann/")
SCHEMA = Namespace("http://schema.org/")

args = sys.argv[1:]
for fpath in args:
    with open(fpath) as fp:
        graph = Graph().parse(fp, format='turtle')

with open(os.path.join(os.path.dirname(__file__), 'vocab-tplt.html')) as f:
    render = PageTemplate(f.read())

def label(obj, lang='sv'):
    label = None
    for label in obj.objects(RDFS.label):
        if label.language == lang:
            return label
    return label

html = render(**vars()).encode('utf-8')
sys.stdout.write(html)
