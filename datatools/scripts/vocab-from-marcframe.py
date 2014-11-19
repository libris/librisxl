import json
from urlparse import urljoin
from rdflib import *
from rdflib.namespace import *
from rdflib.util import guess_format

SDO = Namespace("http://schema.org/")
VANN = Namespace("http://purl.org/vocab/vann/")

BASE = "http://libris.kb.se/"
TERMS = Namespace(BASE + "def/terms#")
DATASET_BASE = Namespace("http://w3id.org/libris/sys/dataset/")
ENUM_BASEPATH = "/def/enum/"

SKIP = ('Other', 'Unspecified', 'Unknown')


def parse_marcframe(dataset, marcframe):

    for part in ['bib', 'auth', 'hold']:
        parse_resourcemap(dataset, part, marcframe)

        g = dataset.get_context(DATASET_BASE["marcframe/fields"])
        for tag, field in marcframe[part].items():
            marc_source = "%s %s" % (part, tag)
            add_terms(g, marc_source, field)

    return g


def parse_resourcemap(dataset, part, marcframe):
    if part != 'bib':
        return # TODO

    basegroup_map = {}

    enumgraph = dataset.get_context(DATASET_BASE["marcframe/enums"])

    def parse_fixed(tag, link, baseclass, groupname):
        for basename, coldefs in marcframe[part][tag].items():
            if basename.startswith('TODO') or not basename[0].isupper():
                continue
            rclass = newclass(enumgraph, basename, baseclass, groupname)
            if isinstance(coldefs, unicode):
                continue
            for coldfn in coldefs.values():
                if coldfn.get('addLink') == link:
                    map_key = coldfn['tokenMap']
                    basegroup_map[map_key] = rclass, groupname

    parse_fixed('008', 'contentType', TERMS.CreativeWork, "content")
    parse_fixed('007', 'carrierType', TERMS.Product, "carrier")


    g = dataset.get_context(DATASET_BASE["marcframe/fixfields"])

    for mapname, dfn in marcframe['tokenMaps'].items():
        if mapname == 'typeOfRecord':
            for name in dfn.values():
                rtype = newclass(g, name, None, "typeOfRecord")
        if mapname == 'bibLevel':
            for name in dfn.values():
                if not name:
                    continue
                if name.lower().endswith(('part', 'unit')):
                    base = TERMS.Part
                elif name in ('MonographItem',
                        'AdministrativePostForLicenseBoundElectronicResource'):
                    base = None
                else:
                    base = TERMS.Aggregate
                if base:
                    rtype = newclass(g, name, base, "bibLevel")
        else:
            basegroup = basegroup_map.get(mapname)

            if not basegroup:
                continue

            for enumref in dfn.values():
                if not enumref or enumref in SKIP or 'Obsolete' in enumref:
                    continue
                stype = newclass(enumgraph, enumref, *basegroup)



def add_terms(g, marc_source, dfn, parentdomain=None):

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

        is_link = ('link' in dfn or 'addLink' in dfn)

        key_is_property = k in ('property', 'addProperty')

        if key_is_property and is_link:
            domainname = dfn.get('resourceType')
            rangename = None
        else:
            domainname = parentdomain
            about = dfn.get('aboutEntity')
            if about == '?record':
                domainname = 'Record'
            elif about == '?instance' or not about and not parentdomain:
                if 'bib' in marc_source.lower():
                    domainname = 'CreativeWork'
            domainname = dfn.get('aboutType') or domainname
            rangename = dfn.get('resourceType')

        marc_source_path = marc_source
        if k.startswith('$'):
            marc_source_path = "%s.%s" % (marc_source, k[1:])
        elif k.startswith('['):
            marc_source_path = marc_source + k
        elif k in ('i1', 'i2'):
            marc_source_path = "%s.%s" % (marc_source, k)

        if not rtypes:
            if not isinstance(v, list):
                v = [v]
            for subdfn in v:
                if isinstance(subdfn, dict):
                    subdomainname = (rangename if
                            is_link and not key_is_property else None)
                    add_terms(g, marc_source_path, subdfn, subdomainname)
            continue

        newprop(g, v, rtypes, domainname, rangename, marc_source_path)


def newclass(g, name, base=None, termgroup=None):
    if ' ' in name:
        rclass = g.resource(BNode())
        rclass.add(RDFS.label, Literal(name, lang='sv'))
    elif name.startswith(ENUM_BASEPATH):
        rclass = g.resource(URIRef(urljoin(BASE, name)))
    else:
        rclass = g.resource(URIRef(TERMS[name]))
    rclass.add(RDF.type, OWL.Class)
    if base:
        rclass.add(RDFS.subClassOf, base)
    if termgroup:
        rclass.add(VANN.termGroup, Literal(termgroup))#ENUM[termgroup])
    return rclass

def newprop(g, name, rtypes, domainname=None, rangename=None, marc_source=None):
    if not name or name in ('@id', '@type'):
        return
    rprop = g.resource(URIRef(TERMS[name]))
    for rtype in rtypes:
        rprop.add(RDF.type, rtype)
    if domainname:
        rprop.add(SDO.domainIncludes, TERMS[domainname])
    if rangename:
        rprop.add(SDO.rangeIncludes, TERMS[rangename])
    if marc_source:
        rprop.add(SKOS.note, Literal("MARC "+ marc_source))
    return rprop


def add_equivalent(dataset, g, refgraph):
    for s in refgraph.subjects():
        try:
            qname = refgraph.qname(s)
        except:
            continue
        if ':' in qname:
            qname = qname.split(':')[-1]
        term = TERMS[qname]
        if (term, None, s) not in dataset:
            if (term, RDF.type, OWL.Class) in dataset:
                rel = OWL.equivalentClass
            elif (term, None, None) in dataset:
                rel = OWL.equivalentProperty
            else:
                continue
            g.add((term, rel, s))


if __name__ == '__main__':
    import sys, os, glob
    args = sys.argv[1:]
    if '-g' in args:
        args.remove('-g')
        fmt = 'trig'
    else:
        fmt = 'turtle'
    source = args.pop(0)
    termspath = args.pop(0) if args else None

    dataset = ConjunctiveGraph()

    with open(source) as fp:
        marcframe = json.load(fp)

    parse_marcframe(dataset, marcframe)

    for refpath in args:
        refgraph = Graph().parse(refpath, format=guess_format(refpath))
        destgraph = dataset.get_context(DATASET_BASE["ext?source=%s" % refpath])
        add_equivalent(dataset, destgraph, refgraph)

    if termspath:
        tg = Graph().parse(termspath, format=guess_format(termspath))
        dataset -= tg
        dataset.remove((None, VANN.termGroup, None))
        dataset.namespace_manager = tg.namespace_manager

    for update_fpath in glob.glob(
            os.path.join(os.path.dirname(__file__), 'vocab-update-*.rq')):
        with open(update_fpath) as fp:
            dataset.update(fp.read())

    dataset.serialize(sys.stdout, format=fmt)
