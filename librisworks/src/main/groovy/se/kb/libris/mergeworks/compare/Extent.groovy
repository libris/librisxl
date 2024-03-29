package se.kb.libris.mergeworks.compare;

class Extent implements FieldHandler {

    // TODO: allow one side missing extent (-1)?
    @Override
    boolean isCompatible(Object a, Object b) {
        return true // a * 0.7 < b && a * 1.3 > b
    }

    @Override
    Object merge(Object a, Object b) {
        return b; // not part of final work
    }
}
