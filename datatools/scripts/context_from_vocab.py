import json
from rdflib import *
from rdflib.resource import Resource
Resource.id = Resource.identifier


CLASS_TYPES = {RDFS.Class, OWL.Class, RDFS.Datatype}
PROP_TYPES = {RDF.Property, OWL.ObjectProperty, OWL.DatatypeProperty}


def context_from_vocab(graph, dest_vocab=None, ns_pref_order=None):
    terms = set()
    for s in graph.subjects():
        if not isinstance(s, URIRef):
            continue
        r = graph.resource(s)
        in_source_vocab = True # TODO: provide source and target vocab(s)
        if in_source_vocab:
            terms.add(r)
    dest_vocab = graph.store.namespace(dest_vocab)
    prefixes = set()
    defs = {}
    for term in terms:
        dfn = termdef(term, ns_pref_order)
        if dfn:
            curie = dfn.get('@reverse') or dfn['@id'] if isinstance(dfn, dict) else dfn
            if ':' in curie:
                pfx = curie.split(':', 1)[0]
                prefixes.add(pfx)
            key = unicode(term.qname())
            if key != dfn:
                defs[key] = dfn
    ns = {}
    for pfx, iri in graph.namespaces():
        if pfx in prefixes:
            ns[pfx] = iri
        elif pfx == "":
            ns["@vocab"] = iri
    if dest_vocab:
        ns["@vocab"] = dest_vocab
    return {"@context": [ns, defs]}

def termdef(term, ns_pref_order=None):
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
    for pred in OWL.sameAs, equiv, subof:
        mapped = get_preferred(term, pred, ns_pref_order)
        if mapped:
            target_term = mapped
            break
    else:
        target_term = None

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

def get_preferred(term, pred, ns_pref_order=None):
    ns_pref_order = ns_pref_order or []
    current, current_index = None, len(ns_pref_order)
    candidate = None
    candidates = list(term.objects(pred))
    term_pfx = _pfx(term)
    for candidate in candidates:
        pfx = _pfx(candidate)
        if pfx == term_pfx:
            candidate = None
            continue
        try:
            index = ns_pref_order.index(pfx)
            if index <= current_index:
                current, current_index = candidate, index
        except ValueError:
            pass
    return current or candidate

def _pfx(term):
    qname = term.qname()
    return qname.split(':', 1)[0] if ':' in qname else ""

def extend(context, overlay):
    ns, defs = context['@context']
    overlay = overlay.get('@context') or overlay
    for term, dfn in overlay.items():
        if isinstance(dfn, basestring) and dfn.endswith(('/', '#', ':', '?')):
            assert term not in ns
            ns[term] = dfn
        elif term in defs:
            v = defs[term]
            if isinstance(v, basestring):
                v = defs[term] = {'@id': v}
            v.update(dfn)
        else:
            defs[term] = dfn


if __name__ == '__main__':
    import sys
    args = sys.argv[1:]
    fpath = args.pop(0)
    overlay_fpath = args.pop(0) if args else None
    dest_vocab = args.pop(0) if args else None
    ns_pref_order = ['dc', 'sdo', 'dctype', 'prov', 'bf']

    fmt = 'n3' if fpath.endswith(('.n3', '.ttl')) else 'xml'
    graph = Graph().parse(fpath, format=fmt)
    context = context_from_vocab(graph, dest_vocab=dest_vocab, ns_pref_order=ns_pref_order)
    if overlay_fpath:
        with open(overlay_fpath) as fp:
            overlay = json.load(fp)
        extend(context, overlay)
    s = json.dumps(context, sort_keys=True, indent=2, separators=(',', ': '),
            ensure_ascii=False).encode('utf-8')
    print s
