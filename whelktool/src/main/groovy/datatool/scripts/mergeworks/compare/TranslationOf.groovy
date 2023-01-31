package datatool.scripts.mergeworks.compare

import datatool.scripts.mergeworks.Util
import datatool.util.DocumentComparator

class TranslationOf implements FieldHandler {
    DocumentComparator c = new DocumentComparator()

    @Override
    boolean isCompatible(Object a, Object b) {
        // @type is sometimes Work, sometimes Text. Should not matter for comparison
        (!a && !b) || (a && b && [Util.asList(a), Util.asList(b)].transpose().every { Map x, Map y ->
            c.isEqual(noTypeNoTitle(x), noTypeNoTitle(y))
            && !Util.getTitleVariants(x['hasTitle']).intersect(Util.getTitleVariants(y['hasTitle'])).isEmpty()
        })
    }

    @Override
    Object merge(Object a, Object b) {
        return a // TODO: prefer one @type over another?
    }

    Map noTypeNoTitle(Map m) {
        m.findAll { k, v -> !(k in ['@type', 'hasTitle']) }
    }


}
