package datatool.scripts.mergeworks.compare

import static datatool.scripts.mergeworks.Util.asList

class ContentType implements FieldHandler {
    Object contentType

    ContentType(String contentType) {
        this.contentType = [['@id' : contentType]]
    }

    @Override
    boolean isCompatible(Object a, Object b) {
        (!a && asList(b) == contentType) || (!b && asList(a) == contentType)
    }

    @Override
    Object merge(Object a, Object b) {
        return a ?: b
    }
}