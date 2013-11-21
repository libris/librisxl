import json
from rdflib import *
from rdflib.resource import Resource
Resource.id = Resource.identifier


CLASS_TYPES = {RDFS.Class, OWL.Class, RDFS.Datatype}
PROP_TYPES = {RDF.Property, OWL.ObjectProperty, OWL.DatatypeProperty}


def context_from_vocab(graph):
    terms = set()
    for s in graph.subjects():
        if not isinstance(s, URIRef):
            continue
        r = graph.resource(s)
        in_source_vocab = True # TODO: provide source and target vocab(s)
        if in_source_vocab:
            terms.add(r)
    prefixes = set()
    defs = {}
    for term in terms:
        dfn = termdef(term)
        if dfn:
            curie = dfn.get('@reverse') or dfn['@id'] if isinstance(dfn, dict) else dfn
            if ':' in curie:
                pfx = curie.split(':', 1)[0]
                prefixes.add(pfx)
            defs[unicode(term.qname())] = dfn
    ns = {}
    for pfx, iri in graph.namespaces():
        if pfx in prefixes:
            ns[pfx] = iri
        elif pfx == "":
            ns["@vocab"] = iri
    return {"@context": [ns, defs]}

def termdef(term):
    types = set(o.id for o in term.objects(RDF.type))
    is_class = types & CLASS_TYPES
    is_prop = types & PROP_TYPES
    if not is_class and not is_prop:
        return None
    if is_class:
        equiv = OWL.equivalentClass
        subof = RDFS.subClassOf
    else:
        equiv = OWL.equivalentProperty
        subof = RDFS.subPropertyOf
    # TODO: get all target candidates, select first based on target vocab order
    target_term = term.value(OWL.sameAs) or term.value(equiv) or term.value(subof)

    curie = unicode((target_term or term).qname())
    if is_class:
        return curie

    range_type = term.value(RDFS.range)
    range_iri = range_type and range_type.id
    if range_iri and range_iri.startswith(XSD) or range_iri == RDFS.Literal:
        datatype = range_type.qname()
    elif OWL.DatatypeProperty in types:
        datatype = False
    else:
        datatype = None

    if types & {RDF.Property, OWL.FunctionalProperty}:
        container = None
    elif range_iri == RDF.List:
        container = "@list"
    #elif OWL.ObjectProperty in types:
    #    container = "@set"
    else:
        container = None

    reverse = None if target_term else term.value(OWL.inverseOf)
    if reverse or datatype or container:
        if reverse:
            dfn = {"@reverse": unicode(reverse.qname())}
        else:
            dfn = {"@id": curie}
        if datatype:
            dfn["@type"] = datatype
        elif datatype is False:
            dfn["@language"] = None
        if container:
            dfn["@container"] = container
        return dfn
    else:
        return curie


if __name__ == '__main__':
    import sys
    args = sys.argv[1:]
    fpath = args.pop(0)
    prefix = args.pop(0) if args else None

    fmt = 'n3' if fpath.endswith(('.n3', '.ttl')) else 'xml'
    graph = Graph().parse(fpath, format=fmt)
    context = context_from_vocab(graph)
    s = json.dumps(context, sort_keys=True, indent=2, separators=(',', ': '),
            ensure_ascii=False).encode('utf-8')
    print s
