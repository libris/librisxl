/**
 * This script moves instanceOf.translationOf into each entity of instanceOf.hasPart provided that certain requirements are met.
 *
 * The requirements are:
 *  - Each of the properties translationOf, hasPart and language are present on the work (instanceOf)
 *  - Each part in hasPart has a title*
 *  - Each part in hasPart has language**
 *  - The language of each part in hasPart matches the work language
 *  - No part in hasPart has translationOf
 *  - work.translationOf has exactly one language
 *  - work.translationOf differs from every hasPart.language
 *
 *  *Except entities that have either of the forms {@type ,language}, {@type ,contentType}, {@type ,genreForm,intendedAudience} or {@type ,genreForm}
 *    These are ignored.
 *  **With a few exceptions, see code.
 *
 *  Unlink any linked part in hasPart.
 *
 *  Remove work.translationOf after it's been copied to hasPart if there is no instanceOf.hasTitle.
 *
 */

import whelk.filter.LanguageLinker

def unhandled = getReportWriter('unhandled.tsv')

def where = """
    collection = 'bib'
    and data #>> '{@graph, 1, instanceOf, translationOf}' is not null
    and data #>> '{@graph, instanceOf, hasPart}' is not null
"""

LANGUAGE = 'language'
HAS_TITLE = 'hasTitle'
HAS_PART = 'hasPart'
INSTANCE_OF = 'instanceOf'
TRANSLATION_OF = 'translationOf'
TYPE = '@type'
ID = '@id'

def exceptShapes = [['@id'], ['@type', 'language'], ['@type', 'contentType'], ['@type', 'genreForm', 'intendedAudience'], ['@type', 'genreForm']]*.toSet()

selectBySqlWhere(where) { bib ->
    def langLinker = bib.whelk.normalizer.normalizers.find { it.linker instanceof LanguageLinker }.linker
    def id = bib.doc.shortId

    def work = bib.graph[1][INSTANCE_OF]
    def hasPart = work[HAS_PART]
    def translationOf = work[TRANSLATION_OF]

    if (asList(translationOf).size() > 1) {
        unhandled.println([id, "multiple translationOf"].join('\t'))
        return
    }

    linkLangs(langLinker, work)
    linkLangs(langLinker, translationOf)
    linkLangs(langLinker, hasPart, asList(work[LANGUAGE]))

    def workLang = asList(work[LANGUAGE])
    def trlOfLang = asList(asList(translationOf)[0][LANGUAGE])

    if (!hasPart.any { it[HAS_TITLE] && it[LANGUAGE] }) {
        return
    }

    if (trlOfLang.size() > 1) {
        unhandled.println([id, "multiple translationOf lang"].join('\t'))
        return
    }

    if (trlOfLang.size() == 0) {
        unhandled.println([id, "missing translationOf lang"].join('\t'))
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

    if (hasPart.any { p -> !(p.keySet() in exceptShapes || p[HAS_TITLE]) }) {
        unhandled.println([id, "missing title"].join('\t'))
        return
    }

    if (hasPart.any { p -> p[LANGUAGE] && !workLang.containsAll(asList(p[LANGUAGE])) }) {
        unhandled.println([id, "language mismatch"].join('\t'))
        return
    }

    def (partsWithLang, partsMissingLang) = hasPart.findAll { p -> !(p.keySet() in exceptShapes) }.split { it[LANGUAGE] }
    def partLangs = partsWithLang.collect { it[LANGUAGE] }.flatten()

    if (partsWithLang && partsMissingLang) {
        if (workLang.size() > 1) {
            def addLang = workLang - partLangs
            if (addLang) {
                if (addLang == trlOfLang) {
                    // There are several work languages
                    // The parts with language are translations of the parts without language
                    // Add original language to the parts missing language
                    hasPart.each { p ->
                        if (!(p.keySet() in exceptShapes)) {
                            p[LANGUAGE] = p[LANGUAGE] ?: addLang
                        }
                    }
                } else {
                    unhandled.println([id, "multiple instanceOf langs"].join('\t'))
                    return
                }
            }
        }
        if (partLangs == workLang) {
            // Add original language to the parts missing language
            hasPart.each { p ->
                if (!(p.keySet() in exceptShapes)) {
                    p[LANGUAGE] = p[LANGUAGE] ?: trlOfLang
                }
            }
        }
    }

    hasPart.each { p ->
        if (p.keySet() in exceptShapes || asList(p[LANGUAGE]) == trlOfLang) {
            return
        }
        p[TRANSLATION_OF] = translationOf
    }

    if (!work[HAS_TITLE]) {
        work.remove(TRANSLATION_OF)
    }

    bib.scheduleSave()
}

def linkLangs(LanguageLinker linker, Object obj, List disambiguationNodes = []) {
    asList(obj).each { entity ->
        if (entity['language']) {
            entity.subMap('language').with {
                linker.linkLanguages(it, disambiguationNodes)
                entity['language'] = it['language']
            }
        }
    }
}

Map loadThing(def id) {
    def thing = [:]
    selectByIds([id]) { t ->
        thing = t.graph[1]
    }
    return thing
}
