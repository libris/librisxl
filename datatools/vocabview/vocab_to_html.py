import sys, os
from rdflib import *
from rdflib.util import guess_format
from rdflib.namespace import SKOS
from jinja2 import Environment, PackageLoader

DC = Namespace("http://purl.org/dc/terms/")
VANN = Namespace("http://purl.org/vocab/vann/")
SCHEMA = Namespace("http://schema.org/")

graph = Graph()

args = sys.argv[1:]
for fpath in args:
    with open(fpath) as fp:
        graph.parse(fp, format=guess_format(fpath))

env = Environment(loader=PackageLoader(__name__, '.'),
        variable_start_string='${', variable_end_string='}',
        line_statement_prefix='%')
tplt = env.get_template('vocab-tplt.html')

def label(obj, lang='sv'):
    label = None
    for label in obj.objects(RDFS.label):
        if label.language == lang:
            return label
    return label

def link(obj):
    if ':' in obj.qname() and not any(obj.objects(None)):
        return obj.identifier
    return '#' + obj.qname()

union = lambda *args: reduce(lambda a, b: a | b, args)

html = tplt.render(dict(vars(__builtins__), **vars())).encode('utf-8')
sys.stdout.write(html)
