package se.kb.libris.mergeworks

import whelk.Document
import whelk.JsonLd
import whelk.Whelk
import whelk.util.DocumentUtil

import static Util.AGENT
import static Util.CLASSIFICATION
import static Util.CODE
import static Util.CONTRIBUTION
import static Util.EDITION_STATEMENT
import static Util.ENCODING_LEVEL
import static Util.EXTENT
import static Util.FAMILY_NAME
import static Util.FLAT_TITLE
import static Util.GIVEN_NAME
import static Util.HAS_TITLE
import static Util.IDENTIFIED_BY
import static Util.IN_SCHEME
import static Util.LABEL
import static Util.LIFE_SPAN
import static Util.NAME
import static Util.PHYS_NOTE
import static Util.PRIMARY
import static Util.PUBLICATION
import static Util.REPRODUCTION_OF
import static Util.RESP_STATEMENT
import static Util.ROLE
import static Util.VERSION
import static whelk.JsonLd.ID_KEY
import static whelk.JsonLd.TYPE_KEY
import static whelk.JsonLd.WORK_KEY


class DisplayDoc {
    Doc doc
    Map framed

    DisplayDoc(Doc doc) {
        this.doc = doc
    }

    private static String displayTitle(Map thing) {
        thing[HAS_TITLE].collect { it[TYPE_KEY] + ": " + it[FLAT_TITLE] }.join(', ')
    }

    String instanceDisplayTitle() {
        displayTitle([(HAS_TITLE): Util.flatTitles(doc.instanceTitle())])
    }

    String getDisplayText(String field) {
        if (field == CONTRIBUTION) {
            return contributorStrings().join("<br>")
        } else if (field == CLASSIFICATION) {
            return classificationStrings().join("<br>")
        } else if (field == 'instance title') {
            return doc.instanceTitle() ?: ''
        } else if (field == 'instance type') {
            return doc.instanceType() ?: ''
        } else if (field == EDITION_STATEMENT) {
            return doc.editionStatement() ?: ''
        } else if (field == RESP_STATEMENT) {
            return doc.responsibilityStatement() ?: ''
        } else if (field == ENCODING_LEVEL) {
            return doc.encodingLevel()
        } else if (field == PUBLICATION) {
            return chipString(doc.publication())
        } else if (field == IDENTIFIED_BY) {
            return chipString(doc.identifiedBy())
        } else if (field == EXTENT) {
            return chipString(doc.extent() ?: [])
        } else if (field == REPRODUCTION_OF) {
            return reproductionOfLink()
        } else if (field == PHYS_NOTE) {
            return doc.physicalDetailsNote() ?: ''
        } else {
            return chipString(doc.workData.getOrDefault(field, []))
        }
    }

    private String chipString(def thing) {
        if (thing instanceof Integer) {
            return thing
        }

        def chips = doc.whelk.jsonld.toChip(thing)
        if (chips.size() < 2) {
            chips = thing
        }
        if (chips instanceof List) {
            return chips.collect { valuesString(it) }.sort().join('<br>')
        }
        return valuesString(chips)
    }

    private String valuesString(def thing) {
        if (thing instanceof List) {
            return thing.collect { valuesString(it) }.join(' • ')
        }
        if (thing instanceof Map) {
            return thing.findAll { k, v -> k != TYPE_KEY }.values().collect { valuesString(it) }.join(' • ')
        }
        return thing.toString()
    }

    private String reproductionOfLink() {
        def base = Document.getBASE_URI().toString()
        def shortId = doc.reproductionOf()
                ? doc.reproductionOf()[0][ID_KEY].substring(base.length()).replace('#it', '')
                : ''
        return "<a href=\"#$shortId\">$shortId</a>"
    }

    String link() {
        String base = Document.getBASE_URI().toString()
        String kat = "katalogisering/"
        String id = doc.document.shortId
        return base + kat + id
    }

    private List contributorStrings() {
        List path = doc.instanceData ? [WORK_KEY, CONTRIBUTION] : [CONTRIBUTION]
        List contribution = DocumentUtil.getAtPath(getFramed(), path, [])

        return contribution.collect { Map c ->
            contributionStr(c)
        }
    }

    private String contributionStr(Map contribution) {
        StringBuilder s = new StringBuilder()

        if (contribution[TYPE_KEY] == PRIMARY) {
            s.append('<b>')
        }

        s.append(flatMaybeLinked(contribution[ROLE], [CODE, LABEL]).with { it.isEmpty() ? it : it + ': ' })
        s.append(flatMaybeLinked(contribution[AGENT], [GIVEN_NAME, FAMILY_NAME, LIFE_SPAN, NAME]))

        if (contribution[TYPE_KEY] == PRIMARY) {
            s.append('</b>')
        }

        return s.toString()
    }

    List classificationStrings() {
        List path = doc.instanceData ? [WORK_KEY, CLASSIFICATION] : [CLASSIFICATION]
        List classification = DocumentUtil.getAtPath(getFramed(), path, [])

        classification.collect { c ->
            StringBuilder s = new StringBuilder()
            s.append(flatMaybeLinked(c[IN_SCHEME], [CODE, VERSION]).with { it.isEmpty() ? it : it + ': ' })
            s.append(flatMaybeLinked(c, [CODE]))
            return s.toString()
        }
    }

    private static String flatMaybeLinked(Object thing, List order) {
        if (!thing)
            return ''

        if (thing instanceof List) {
            return thing.collect { flatMaybeLinked(it, order) }.join(' | ')
        }
        String s = flatten(thing, order, ', ')

        thing[ID_KEY]
                ? """<a href="${thing[ID_KEY]}">$s</a>"""
                : s
    }

    static String flatten(Object o, List order, String mapSeparator = ': ') {
        if (o instanceof String) {
            return o
        }
        if (o instanceof List) {
            return o
                    .collect { flatten(it, order) }
                    .join(' || ')
        }
        if (o instanceof Map) {
            return order
                    .findResults { ((Map) o).get(it) }
                    .collect { flatten(it, order) }
                    .join(mapSeparator)
        }

        throw new RuntimeException(String.format("unexpected type: %s for %s", o.class.getName(), o))
    }

    Map getFramed() {
        if (!framed) {
            Document copy = doc.document.clone()
            doc.whelk.embellish(copy)
            framed = JsonLd.frame(doc.thingIri(), copy.data)
        }

        return framed
    }
}
