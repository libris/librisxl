package datatool.scripts.mergeworks.compare

import datatool.scripts.mergeworks.Util

class WorkTitle implements FieldHandler {

    @Override
    boolean isCompatible(Object a, Object b) {
        return !a || !b || !Util.getTitleVariants(a).intersect(Util.getTitleVariants(b)).isEmpty()
    }

    @Override
    Object merge(Object a, Object b) {
        return a ?: b
    }
}
