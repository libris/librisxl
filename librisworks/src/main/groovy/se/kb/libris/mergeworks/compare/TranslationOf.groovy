package se.kb.libris.mergeworks.compare

import whelk.datatool.util.DocumentComparator
import se.kb.libris.mergeworks.Doc
import se.kb.libris.mergeworks.Util
import org.apache.commons.lang3.NotImplementedException

class TranslationOf implements ValuePicker {
    DocumentComparator c = new DocumentComparator()

    @Override
    boolean isCompatible(Object a, Object b) {
        // @type is sometimes Work, sometimes Text. Should not matter for comparison
        // We assume that there are never more than one object in translationOf
        a = Util.asList(a)[0]
        b = Util.asList(b)[0]
        a && b && c.isEqual(noTypeNoTitle(a), noTypeNoTitle(b)) && noTitleOrSameTitle(a, b)
    }

    @Override
    Object merge(Object a, Object b) {
        throw new NotImplementedException('')
    }

    @Override
    Object pick(Collection<Doc> values) {
        // TODO: which title to pick when matched with already existing linked work?
        def translationOf = values.first().workData['translationOf']
        def title = Util.bestOriginalTitle(values)
        if (title) {
            Util.asList(translationOf)[0]['hasTitle'] = title
        }

        return translationOf
    }

    Map noTypeNoTitle(Map m) {
        m.findAll { k, v -> !(k in ['@type', 'hasTitle']) }
    }

    boolean noTitleOrSameTitle(Map a, Map b) {
        !a['hasTitle']
                || !b['hasTitle']
                || !Util.getFlatTitle(a['hasTitle']).intersect(Util.getFlatTitle(b['hasTitle'])).isEmpty()
    }
}
