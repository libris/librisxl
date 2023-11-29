package se.kb.libris.mergeworks.compare

import datatool.util.DocumentComparator
import se.kb.libris.mergeworks.Doc
import se.kb.libris.mergeworks.Util
import org.apache.commons.lang3.NotImplementedException

import static se.kb.libris.mergeworks.Util.HAS_TITLE
import static se.kb.libris.mergeworks.Util.TRANSLATION_OF
import static whelk.JsonLd.TYPE_KEY

class TranslationOf implements ValuePicker {
    DocumentComparator c = new DocumentComparator()

    @Override
    boolean isCompatible(Object a, Object b) {
        // @type is sometimes Work, sometimes Text. Should not matter for comparison
        // We assume that there is never more than one object in translationOf
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
        def linkedWorkTranslationOf = docs.findResult { it.workIri() ? it.translationOf() : null }
        if (linkedWorkTranslationOf) {
            return linkedWorkTranslationOf
        }
        def translationOf = values.first().workData[TRANSLATION_OF]
        def title = Util.bestOriginalTitle(values)
        if (title) {
            Util.asList(translationOf)[0][HAS_TITLE] = title
        }

        return translationOf
    }

    Map noTypeNoTitle(Map m) {
        m.findAll { k, v -> !(k in [TYPE_KEY, HAS_TITLE]) }
    }

    boolean noTitleOrSameTitle(Map a, Map b) {
        !a[HAS_TITLE]
                || !b[HAS_TITLE]
                || !Util.getFlatTitle(a[HAS_TITLE]).intersect(Util.getFlatTitle(b[HAS_TITLE])).isEmpty()
    }
}
