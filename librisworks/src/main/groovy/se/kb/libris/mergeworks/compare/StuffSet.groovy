package se.kb.libris.mergeworks.compare


import java.util.function.BiFunction

import static se.kb.libris.mergeworks.Util.asList

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
        boolean changed = false
        List result = []
        asList(o).each {
            def merged = null
            for (int i = 0 ; i < result.size() ; i++) {
                merged = s.apply(result[i], it)
                if (merged) {
                    result[i] = merged
                    changed = true
                    break
                }
            }
            if (merged == null) {
                result << it
            }
        }
        return changed ? mergeCompatibleElements(result, s) : result
    }
}