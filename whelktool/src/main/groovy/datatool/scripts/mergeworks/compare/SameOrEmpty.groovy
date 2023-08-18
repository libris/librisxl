package datatool.scripts.mergeworks.compare

import static datatool.scripts.mergeworks.Util.asList

class SameOrEmpty implements FieldHandler {
    Object link

    SameOrEmpty(String iri) {
        this.link = [['@id': iri]]
    }

    @Override
    boolean isCompatible(Object a, Object b) {
        (!a && asList(b) == link) || (!b && asList(a) == link)
    }

    @Override
    Object merge(Object a, Object b) {
        return a ?: b
    }
}