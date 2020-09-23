package datatool.scripts.mergeworks.compare

interface FieldHandler {
    boolean isCompatible(Object a, Object b)
    Object merge(Object a, Object b)
}