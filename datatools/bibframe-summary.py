from rdflib import *


BF = Namespace("http://bibframe.org/vocab/")
ABS = Namespace("http://bibframe.org/model-abstract/")

def print_bibframe(g):
    g.namespace_manager.bind(None, BF)
    global otherclasses, otherprops
    otherclasses = set(g.subjects(RDF.type, RDFS.Class))
    otherprops = set(g.subjects(RDF.type, ABS.BFProperty))
    print_class(g.resource(BF.Resource))
    #for c in sorted(otherclasses):
    #    if not g.value(c, RDFS.subClassOf):
    #        print_class(g.resource(c))
    for c in sorted(otherclasses):
        print_class(g.resource(c))
    if otherprops:
        print "#Other"
        for p in sorted(otherprops):
            print_propsum(g.resource(p), None, "   ")

def print_class(c, superclasses=set()):
    indent = "    " * len(superclasses)
    subnote = "/ " if superclasses else ""
    superclasses = superclasses | {c}
    otherclasses.discard(c.identifier)
    print indent + subnote + c.qname()
    props = sorted(c.subjects(RDFS.domain))
    for prop in props:
        if any(prop.objects(RDFS.subPropertyOf)):
            continue
        otherprops.discard(prop.identifier)
        lbl = prop.qname()
        ranges = tuple(prop.objects(RDFS.range))
        if ranges:
            lbl += " => " + ", ".join(rc.qname() for rc in ranges)
        marc = prop.value(ABS.marcField)
        if marc:
            lbl += " # " + marc
        print indent + "    " + lbl
        print_subproperties(prop, c, "       ")
    for subc in sorted(c.subjects(RDFS.subClassOf)):
        if subc in superclasses:
            print indent, "<=", subc.qname()
            continue
        print_class(subc, superclasses)
    if props:
        print

def print_subproperties(prop, domain, indent):
    for subprop in sorted(prop.subjects(RDFS.subPropertyOf)):
        otherprops.discard(subprop.identifier)
        print_propsum(subprop, domain, indent)
        print_subproperties(subprop, domain, indent + "    ")

def print_propsum(subprop, domain, indent):
    lbl = subprop.qname()
    subpropdomains = sorted(subprop.objects(RDFS.domain))
    if subpropdomains and subpropdomains != [domain]:
        lbl += " of " + ", ".join(spd.qname() for spd in subpropdomains)
    marc = subprop.value(ABS.marcField)
    if marc:
        lbl += " # " + marc
    print indent, lbl


if __name__ == '__main__':
    from sys import argv
    src = argv[1]
    g = Graph().parse(src, format="turtle")
    print_bibframe(g)
