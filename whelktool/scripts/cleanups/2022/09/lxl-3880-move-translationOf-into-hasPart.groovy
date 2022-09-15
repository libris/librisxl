/**
 * This script moves instanceOf.translationOf into each entity of instanceOf.hasPart provided that certain requirements are met.
 *
 * The requirements are:
 *  - Each of the properties translationOf, hasPart and language are present on the work (instanceOf)
 *  - Each part in hasPart has a title and exactly one language**
 *  - The work has exactly language
 *  - The language of each part in hasPart matches the work language
 *  - No part in hasPart has translationOf
 *  - work.translationOf has exatly one language
 *  - work.translationOf differs from every hasPart.language
 *
 *  **Except entities that have either of the forms {@type, contentType}, {@type, genreForm, intendedAudience} or {@type, genreForm}
 *    These are ignored.
 *
 *  Unlink any linked part in hasPart.
 *
 *  Remove work.translationOf after it's been copied to hasPart.
 *
 */

def unhandled = getReportWriter('unhandled.tsv')

def where = """
    collection = 'bib'
    AND data['@graph'][1]['instanceOf']['translationOf'] IS NOT NULL
    AND data['@graph'][1]['instanceOf']['hasPart'] IS NOT NULL
"""

LANGUAGE = 'language'
HAS_TITLE = 'hasTitle'
HAS_PART = 'hasPart'
INSTANCE_OF = 'instanceOf'
TRANSLATION_OF = 'translationOf'
TYPE = '@type'
ID = '@id'

def exceptShapes = [['@type', 'contentType'], ['@type', 'genreForm', 'intendedAudience'], ['@type', 'genreForm']]*.toSet()

selectBySqlWhere(where) { bib ->
    def id = bib.doc.shortId

    def work = bib.graph[1][INSTANCE_OF]
    def hasPart = work[HAS_PART]
    def translationOf = work[TRANSLATION_OF]

    def workLang = asList(work[LANGUAGE])

    hasPart.each { p ->
        if (p[ID]) {
            def t = loadThing(p[ID])
            p.clear()
            p.putAll(t.subMap([TYPE, HAS_TITLE, LANGUAGE]))
        }
    }

    if (!hasPart.any { it[HAS_TITLE] && it[LANGUAGE] }) {
        return
    }

    if (asList(translationOf).size() > 1) {
        unhandled.println([id, "multiple translationOf"].join('\t'))
        return
    }

    def trlOfLang = asList(asList(translationOf)[0][LANGUAGE])

    if (workLang.size() != 1 || trlOfLang.size() != 1) {
        unhandled.println([id, "multiple langs"].join('\t'))
        return
    }

    if (hasPart.any { p -> p.containsKey(TRANSLATION_OF) }) {
        unhandled.println([id, "translationOf already exists"].join('\t'))
        return
    }

    if (hasPart.any { p -> asList(p[LANGUAGE]) == trlOfLang }) {
        unhandled.println([id, "language = translationOf.language"].join('\t'))
        return
    }

    if (hasPart.any { p -> !(p.keySet() in exceptShapes) && !p[HAS_TITLE] }) {
        unhandled.println([id, "missing title"].join('\t'))
        return
    }

    if (hasPart.any { p -> !(p.keySet() in exceptShapes) && asList(p[LANGUAGE]) != workLang }) {
        unhandled.println([id, "language mismatch"].join('\t'))
        return
    }

    hasPart.each { p ->
        if (p.keySet() in exceptShapes) {
            return
        }
        p[TRANSLATION_OF] = translationOf
    }

    work.remove(TRANSLATION_OF)

    bib.scheduleSave()
}

private List asList(Object o) {
    if (o == null)
        return []
    if (o instanceof List)
        return o
    return [o]
}

Map loadThing(def id) {
    def thing = [:]
    selectByIds([id]) { t ->
        thing = t.graph[1]
    }
    return thing
}
