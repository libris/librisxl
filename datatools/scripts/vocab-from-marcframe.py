import json
from rdflib import *

SDO = Namespace("http://schema.org/")

TERMS = Namespace("http://libris.kb.se/def/terms#")

SKIP = ('Other', 'Unspecified', 'Unknown')


def parse_marcframe(marcframe):
    g = Graph()
    g.bind('', TERMS)
    g.bind('owl', OWL)
    g.bind('sdo', SDO)

    for k, v in marcframe['compositionTypeMap'].items():
        rtype = newclass(g, k, TERMS.Part if k.endswith('Part') else TERMS.Composite)

    for k, v in marcframe['contentTypeMap'].items():
        rtype = newclass(g, k, TERMS.Work)
        for sk in v['subclasses']:
            if sk in SKIP: continue
            if 'Obsolete' in sk: continue
            stype = newclass(g, sk, rtype, TERMS.Work)
        for sk in v.get('formClasses', ()):
            if sk in SKIP: continue
            stype = newclass(g, sk, rtype)

    for k, v in marcframe['carrierTypeMap'].items():
        rtype = newclass(g, k, TERMS.Instance)
        for sk in v:
            if sk in SKIP: continue
            stype = newclass(g, sk, rtype, TERMS.Carrier)

    def add_terms(dfn):
        for k, v in dfn.items():
            if not v:
                continue
            if k == 'defaults':
                for dp in v:
                    newprop(g, dp, {RDF.Property})
                continue

            rtypes = {
                'property': {OWL.DatatypeProperty, OWL.FunctionalProperty},
                'addProperty': {OWL.DatatypeProperty},
                'link': {OWL.ObjectProperty, OWL.FunctionalProperty},
                'addLink': {OWL.ObjectProperty},
            }.get(k)

            if not rtypes:
                if not isinstance(v, list):
                    v = [v]
                for subdfn in v:
                    if isinstance(subdfn, dict):
                        add_terms(subdfn)
                continue

            domainname = dfn.get('domainEntity')
            rangename = dfn.get('rangeEntity')
            if k == 'property' and 'link' in dfn or 'addLink' in dfn:
                domainname = rangename = None

            newprop(g, v, rtypes, domainname, rangename)

    for part in ['bib', 'auth', 'hold']:
        for field in marcframe[part].values():
            add_terms(field)

    return g


def newclass(g, name, *bases):
    if ' ' in name:
        rclass = g.resource(BNode())
        rclass.add(RDFS.label, Literal(name, lang='sv'))
    else:
        rclass = g.resource(URIRef(TERMS[name]))
    rclass.add(RDF.type, OWL.Class)
    for base in bases:
        rclass.add(RDFS.subClassOf, base)
    return rclass

def newprop(g, name, rtypes, domainname=None, rangename=None):
    if not name:
        return
    rprop = g.resource(URIRef(TERMS[name]))
    for rtype in rtypes:
        rprop.add(RDF.type, rtype)
    if domainname:
        rprop.add(SDO.domainIncludes, TERMS[domainname])
    if rangename:
        rprop.add(SDO.rangeIncludes, TERMS[rangename])
    return rprop


if __name__ == '__main__':
    import sys
    args = sys.argv[1:]
    source = args.pop(0)

    with open(source) as fp:
        marcframe = json.load(fp)

    g = parse_marcframe(marcframe)

    g.serialize(sys.stdout, format='turtle')
