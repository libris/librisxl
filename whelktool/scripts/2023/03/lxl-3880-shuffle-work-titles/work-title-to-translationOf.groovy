/**
 * Move titles in original language to translationOf for all work types except Music and NotatedMusic.
 * Also move legalDate (if exists, only a few cases) along with the title.
 * Construct translationOf when a work obviously is a translation (has a translator) but lack the property.
 */

moved = getReportWriter('moved.tsv')
propertyAlreadyExists = getReportWriter('property-already-exists.tsv')
hasFieldRef = getReportWriter('has-fieldref.txt')

HAS_TITLE = 'hasTitle'
TRANSLATION_OF = 'translationOf'

TITLE_RELATED_PROPS = ['hasTitle', 'legalDate']

def where = """
    collection = 'bib'
        and (data #>> '{@graph, 1, instanceOf, translationOf}' is not null
            or data #>> '{@graph, 1, instanceOf, hasPart}' is not null
            or data #>> '{@graph, 1, instanceOf, contribution}' LIKE '%"https://id.kb.se/relator/translator"%')
"""

moved.println(['id', 'location', 'moved properties', 'translationOf', 'has original language'].join('\t'))

selectBySqlWhere(where) {
    if (moveTitlesRecursive(it.graph[1].instanceOf, it.doc.shortId, 'instanceOf')) {
        it.scheduleSave()
    }
}

boolean moveTitlesRecursive(Object o, String id, String via) {
    def successfulMove = false

    asList(o).each {
        if (it instanceof Map) {
            successfulMove |= tryMoveTitle(it, id, via)
            it.each { k, v ->
                successfulMove |= moveTitlesRecursive(v, id, k)
            }
        }
    }

    return successfulMove
}

boolean tryMoveTitle(Map work, String id, String via) {
    def moveThese = work.keySet().intersect(TITLE_RELATED_PROPS)

    if (!moveThese.contains(HAS_TITLE)
            || asList(work[TRANSLATION_OF]).size() > 1
            || work['@type'] in ['Music', 'NotatedMusic']
    ) {
        return false
    }

    if (!work[TRANSLATION_OF]) {
        if (hasTranslator(work)) {
            work[TRANSLATION_OF] = ['@type': 'Work']
        } else {
            return false
        }
    }

    if (work['marc:fieldref']) {
        hasFieldRef.println(id)
        return false
    }

    def translationOf = asList(work[TRANSLATION_OF])[0]
    def conflictingProps = moveThese.intersect(translationOf.keySet())

    if (conflictingProps.isEmpty()) {
        work.removeAll { k, v ->
            if (k in moveThese) {
                translationOf[k] = v
                return true
            }
        }
        work.remove('musicKey')
        moved.println([id, via, moveThese, translationOf, translationOf['language'].asBoolean()].join('\t'))
        return true
    }

    propertyAlreadyExists.println([id, via, conflictingProps].join('\t'))
    return false
}

boolean hasTranslator(Map work) {
    asList(work.contribution).any { Map c ->
        asList(c.role).contains(['@id': 'https://id.kb.se/relator/translator'])
    }
}