package mergeworks.compare

import mergeworks.Doc
import org.apache.commons.lang3.NotImplementedException

class Id implements ValuePicker {

    @Override
    boolean isCompatible(Object a, Object b) {
        return true
    }

    @Override
    Object merge(Object a, Object b) {
        throw new NotImplementedException('')
    }

    @Override
    Object pick(Collection<Doc> values) {
        return values.findResult { it.workIri() }
    }
}
