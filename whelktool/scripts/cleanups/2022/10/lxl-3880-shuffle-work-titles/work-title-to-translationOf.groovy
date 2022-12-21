moved = getReportWriter('moved.tsv')
propertyAlreadyExists = getReportWriter('property-already-exists.tsv')

HAS_TITLE = 'hasTitle'
TRANSLATION_OF = 'translationOf'

TITLE_RELATED_PROPS = ['hasTitle', 'musicKey', 'musicMedium', 'version', 'marc:version', 'marc:fieldref', 'legalDate', 'originDate']

def where = """
    collection = 'bib'
        and data['@graph'][1]['instanceOf']['translationOf'] is not null
"""

selectBySqlWhere(where) {
    if (moveTitlesRecursive(it.graph[1].instanceOf, it.doc.shortId, 'instanceOf')) {
        it.scheduleSave()
    }
}

boolean moveTitlesRecursive(Object o, String id, String via) {
    def successfulMove = false

    asList(o).each {
        if (it instanceof Map) {
            successfulMove = tryMoveTitle(it, id, via)
            it.each { k, v ->
                successfulMove |= moveTitlesRecursive(v, id, k)
            }
        }
    }

    return successfulMove
}

boolean tryMoveTitle(Map work, String id, String via) {
    def moveThese = work.keySet().intersect(TITLE_RELATED_PROPS) //TODO: Maybe keep some properties on work, needs further analysis

    if (!moveThese.contains(HAS_TITLE)
            || asList(work[TRANSLATION_OF]).size() != 1
            || work['@type'] in ['Music', 'NotatedMusic']
    ) {
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
//        normalizePunctuation(translationOf[HAS_TITLE])
        moved.println([id, via, moveThese, translationOf].join('\t'))
        return true
    }

    propertyAlreadyExists.println([id, via, conflictingProps].join('\t'))
    return false
}

void normalizePunctuation(Object title) {
    // TODO
}