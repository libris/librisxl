package whelk.WorkMerging.compare

import whelk.WorkMerging.Doc
import whelk.WorkMerging.Util
import org.apache.commons.lang3.NotImplementedException

class WorkTitle implements ValuePicker {

    @Override
    boolean isCompatible(Object a, Object b) {
        return !a || !b || !Util.getTitleVariants(a).intersect(Util.getTitleVariants(b)).isEmpty()
    }

    @Override
    Object merge(Object a, Object b) {
        throw new NotImplementedException('')
    }
    
    @Override
    Object pick(Collection<Doc> values) {
        return Util.bestTitle(values)
    }
}
