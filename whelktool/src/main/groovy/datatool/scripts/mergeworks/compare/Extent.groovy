package datatool.scripts.mergeworks.compare;

class Extent implements FieldHandler {

    @Override
    boolean isCompatible(Object a, Object b) {
        return a * 0.8 < b && a * 1.2 > b
    }

    @Override
    Object merge(Object a, Object b) {
        return b; // not part of final work
    }
}
