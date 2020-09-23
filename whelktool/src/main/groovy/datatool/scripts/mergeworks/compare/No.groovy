package datatool.scripts.mergeworks.compare

class No implements Comp {
    @Override
    boolean isCompatible(Object a, Object b) {
        return false
    }

    @Override
    Object merge(Object a, Object b) {
        throw new RuntimeException()
    }
}