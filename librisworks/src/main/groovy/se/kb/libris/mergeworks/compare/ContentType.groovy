package se.kb.libris.mergeworks.compare

import static se.kb.libris.mergeworks.Util.asList
import static whelk.JsonLd.ID_KEY

class ContentType extends StuffSet {
    private static def allowedValues = ['https://id.kb.se/term/rda/StillImage', 'https://id.kb.se/term/rda/Text']

    @Override
    boolean isCompatible(Object a, Object b) {
        asList(a).every { it[ID_KEY] in allowedValues } && asList(b).every { it[ID_KEY] in allowedValues }
    }
}
