package whelk.WorkMerging.compare

import whelk.WorkMerging.Doc

interface FieldHandler {
    boolean isCompatible(Object a, Object b)
    Object merge(Object a, Object b)
}

interface ValuePicker extends FieldHandler {
    Object pick(Collection<Doc> values)
}