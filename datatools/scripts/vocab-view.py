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

def in_vocab(r):
    return ':' not in r.qname()

def make_vocab_tree(g, lang):
    topclasses = []
    for rclass in g.resource(OWL.Class).subjects(RDF.type):
        if any(pc for pc in rclass.objects(RDFS.subClassOf) if in_vocab(pc)):
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

def make_vocab_graph(g, lang):
    nodes = []
    namemap = {}
    for rclass in g.resource(OWL.Class).subjects(RDF.type):
        name = get_name(rclass, lang)
        node = {
            'name': name,
            'children': [],
            'siblingCount': 0
        }
        nodes.append(node)
        namemap[name] = rclass, node

    nodes.sort(key=lambda node: node['name'])
    for i, node in enumerate(nodes):
        node['i'] = i
    links = []
    for node in nodes:
        rclass = namemap[node['name']][0]
        top_base = list(bc for bc in rclass.objects(RDFS.subClassOf*'*')
                if in_vocab(bc))[-1]
        group_name = get_name(top_base, lang)
        node['groupName'] = group_name
        node['group'] = namemap[group_name][1]['i']
        is_base = True
        for bc in rclass.objects(RDFS.subClassOf):
            if not in_vocab(bc):
                continue
            is_base = False
            basenode = namemap[get_name(bc, lang)][1]
            links.append({'source': node['i'], 'target': basenode['i']})
            basenode['children'].append(node['i'])
            node['siblingCount'] += len(basenode['children'])
            continue
        node['base'] = is_base
    return {
        'nodes': nodes,
        'links': links
    }


if __name__ == '__main__':
    import sys
    args = sys.argv[1:]
    kind = args.pop(0)
    lang = 'sv'
    g = to_graph(args)
    if kind == 'tree':
        data = make_vocab_tree(g, lang)
    else:
        data = make_vocab_graph(g, lang)
    print json.dumps(data, sort_keys=True, ensure_ascii=False,
            indent=2, separators=(',', ': ')).encode('utf-8')
