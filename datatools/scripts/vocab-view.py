import json
from rdflib import *
from rdflib.util import guess_format


def to_graph(paths):
    g = Graph()
    for fpath in paths:
        g.parse(fpath, format=guess_format(fpath))
    return g

def get_name(r, lang):
    for label in []:#r.objects(RDFS.label):
        name = unicode(label)
        if label.language == lang:
            break
    else:
        name = r.qname()
    return name

def in_vocab(r, vocab):
    if not vocab.endswith(':'):
        r.graph.bind('', URIRef(vocab))
    if isinstance(r.identifier, BNode):
        return False
    return ':' not in r.qname() or r.qname().startswith(vocab)


def make_vocab_tree(g, lang, vocab):
    topclasses = []
    for rclass in g.resource(OWL.Class).subjects(RDF.type):
        if any(pc for pc in rclass.objects(RDFS.subClassOf) if in_vocab(pc, vocab)):
            continue
        topclasses.append(tree_node(rclass, lang))
    return {
        'name': 'Thing',
        'children': topclasses
    }

def tree_node(rclass, lang):
    name = get_name(rclass, lang)
    children = [tree_node(sc, lang) for sc in rclass.subjects(RDFS.subClassOf)]
    node = {'name': name}
    if children:
        node['children'] = children
    return node


def make_vocab_graph(g, lang, vocab):
    nodes = []
    classes = set(g.resource(r) for r, o in g.subject_objects(RDF.type)
            if o in {RDFS.Class, OWL.Class})
    for rclass in classes:
        if not in_vocab(rclass, vocab):
            continue
        name = get_name(rclass, lang)
        child_names = [get_name(sc, lang) for sc in rclass.subjects(RDFS.subClassOf)]
        node = {'name': name, 'children': child_names}
        nodes.append(node)

    return {'nodes': sorted(nodes, key=lambda node: node['name'])}


if __name__ == '__main__':
    import sys
    args = sys.argv[1:]
    kind = args.pop(0)
    lang = 'sv'
    vocab = args.pop(0)
    g = to_graph(args)
    if kind == 'tree':
        data = make_vocab_tree(g, lang, vocab)
    else:
        data = make_vocab_graph(g, lang, vocab)
    print json.dumps(data, sort_keys=True, ensure_ascii=False,
            indent=2, separators=(',', ': ')).encode('utf-8')
