package whelk.search2.querytree

import whelk.search2.Operator

class DummyNodes {
    static def eq = Operator.EQUALS
    static def neq = Operator.NOT_EQUALS
    static def gt = Operator.GREATER_THAN
    static def lt = Operator.LESS_THAN

    static def path1 = new Path(new Key.RecognizedKey('p1'))
    static def path2 = new Path(new Key.RecognizedKey('p2'))
    static def path3 = new Path(new Key.RecognizedKey('p3'))

    static def prop1 = new Property('p1', ['prefLabel': 'p1'], null)
    static def prop2 = new Property('p:2', ['prefLabel': 'p2'], 'p2')
    static def prop3 = new Property('p3', ['prefLabel': 'p3'], null)

    static def v1 = new Literal('v1')
    static def v2 = new Link('v:2', ['prefLabel': 'v2'])
    static def v3 = new VocabTerm('v3', ['prefLabel': 'v3'])

    static def pathV1 = new PathValue(path1, eq, v1)
    static def pathV2 = new PathValue(path2, eq, v2)
    static def pathV3 = new PathValue(path3, eq, v3)
    static def pathV4 = new PathValue(path1, eq, v2)
    static def pathV5 = new PathValue(path2, eq, v1)

    static def notPathV1 = new PathValue(path1, neq, v1)
    static def notPathV2 = new PathValue(path2, neq, v2)
    static def notPathV3 = new PathValue(path3, neq, v3)

    static def propV1 = new PathValue(new Path(prop1), eq, v1)
    static def propV2 = new PathValue(new Path(prop2), eq, v2)
    static def propV3 = new PathValue(new Path(prop3), eq, v3)

    static def orXY = new Or([pathV1, pathV2])
    static def andXY = new And([pathV1, pathV2])
    static def andXYZ = new And([pathV1, pathV2, pathV3])
    static def notXY = new And([notPathV1, notPathV2])

    static def ft1 = new FreeText('ft1')

    static def type1 = new PathValue(new Path(new Property("rdf:type", [:], null)), Operator.EQUALS, new VocabTerm("T1", [:]))
    static def type2 = new PathValue(new Path(new Property("rdf:type", [:], null)), Operator.EQUALS, new VocabTerm("T2", [:]))

    static def ft(String s) {
        return new FreeText(s)
    }

    static def notFt(String s) {
        return new FreeText(null, neq, s)
    }

    static def propV(Property p, Operator op, Value v) {
        return new PathValue(p, op, v)
    }

    static def propV(String p) {
        propV(new Property(p, [:], null), eq, v1)
    }

    static def pathV(Path p, Operator op, Value v) {
        return new PathValue(p, op, v)
    }

    static def pathV(Operator op) {
        return new PathValue('p', op, new Literal('v'))
    }

    static def pathV(String p) {
        return new PathValue(p, eq, v1)
    }

    static def abf(String alias, Node filter, Map prefLabelByLang) {
        return new ActiveFilter(alias, filter, prefLabelByLang)
    }

    static def and(List l) {
        return new And(l)
    }

    static def or(List l) {
        return new Or(l)
    }
}
