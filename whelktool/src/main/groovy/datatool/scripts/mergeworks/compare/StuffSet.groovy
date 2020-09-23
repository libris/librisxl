package datatool.scripts.mergeworks.compare

import datatool.scripts.mergeworks.Util

import java.util.function.BiFunction

class StuffSet implements FieldHandler {
    @Override
    boolean isCompatible(Object a, Object b) {
        true
    }

    @Override
    Object merge(Object a, Object b) {
        return ((asList(a) as Set) + (asList(b) as Set)).collect()
    }
    
    static Object mergeCompatibleElements(Object o, BiFunction<Object, Object, Object> s) {
        List result = []
        Util.asList(o).each {
            def merged = null
            for (int i = 0 ; i < result.size() ; i++) {
                merged = s.apply(result[i], it)
                if (merged) {
                    result[i] = merged
                    break
                }
            }
            if (!merged) {
                result << it
            }
        }
    }
}