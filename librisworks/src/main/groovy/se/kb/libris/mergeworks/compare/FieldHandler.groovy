package se.kb.libris.mergeworks.compare

import se.kb.libris.mergeworks.Doc

interface FieldHandler {
    boolean isCompatible(Object a, Object b)
    Object merge(Object a, Object b)
}

interface ValuePicker extends FieldHandler {
    Object pick(Collection<Doc> values)
}