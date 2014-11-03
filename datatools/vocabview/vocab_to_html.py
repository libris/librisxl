import sys, os
import urllib2
from rdflib import *
from rdflib.util import guess_format
from rdflib.namespace import SKOS
from jinja2 import Environment, PackageLoader


DC = Namespace("http://purl.org/dc/terms/")
VANN = Namespace("http://purl.org/vocab/vann/")
SCHEMA = Namespace("http://schema.org/")

vocab_url_map = {str(SCHEMA): "http://schema.org/docs/schema_org_rdfa.html"}

graph = Graph()


args = sys.argv[1:]
vocabcache = args.pop(0) if len(args) > 1 else None
for fpath in args:
    with open(fpath) as fp:
        graph.parse(fp, format=guess_format(fpath))


extgraph = Graph()
if vocabcache:
    if not os.path.isdir(vocabcache):
        os.makedirs(vocabcache)
    for vocab in graph.objects(None, OWL.imports):
        fpath = os.path.join(vocabcache, urllib2.quote(vocab, safe="")) + '.ttl'
        if os.path.exists(fpath):
            extgraph.parse(fpath, format='turtle')
        else:
            vocab_url = vocab_url_map.get(str(vocab), vocab)
            print "Fetching", vocab_url, "to", fpath
            g = Graph().parse(vocab_url)
            with open(fpath, 'w') as f:
                g.serialize(f, format='turtle')
            extgraph += g


def getrestrictions(rclass):
    for c in rclass.objects(RDFS.subClassOf):
        rtype = c.value(RDF.type)
        if rtype and rtype.identifier == OWL.Restriction:
            yield c

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

def listclass(o):
    return 'ext' if ':' in o.qname() else ''

union = lambda *args: reduce(lambda a, b: a | b, args)


env = Environment(loader=PackageLoader(__name__, '.'),
        variable_start_string='${', variable_end_string='}',
        line_statement_prefix='%')
tplt = env.get_template('vocab-tplt.html')

html = tplt.render(dict(vars(__builtins__), **vars())).encode('utf-8')
sys.stdout.write(html)
