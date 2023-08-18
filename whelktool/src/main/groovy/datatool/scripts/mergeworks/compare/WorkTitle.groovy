package datatool.scripts.mergeworks.compare

import datatool.scripts.mergeworks.Doc
import datatool.scripts.mergeworks.Util
import org.apache.commons.lang3.NotImplementedException

class WorkTitle implements ValuePicker {

    @Override
    boolean isCompatible(Object a, Object b) {
        return !a || !b || !Util.getFlatTitle(a).intersect(Util.getFlatTitle(b)).isEmpty()
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
