package datatool.scripts.mergeworks.compare

interface Comp {
    boolean isCompatible(Object a, Object b)
    Object merge(Object a, Object b)
}