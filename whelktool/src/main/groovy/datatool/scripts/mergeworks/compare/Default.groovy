package datatool.scripts.mergeworks.compare

class Default implements FieldHandler {
    @Override
    boolean isCompatible(Object a, Object b) {
        return false
    }

    @Override
    Object merge(Object a, Object b) {
        return a
    }
}