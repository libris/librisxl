package datatool.scripts.mergeworks.compare

import datatool.scripts.mergeworks.Doc

interface FieldHandler {
    boolean isCompatible(Object a, Object b)
    Object merge(Object a, Object b)
}

interface ValuePicker extends FieldHandler {
    Object pick(List<Tuple2<Doc, Object>> values)
}