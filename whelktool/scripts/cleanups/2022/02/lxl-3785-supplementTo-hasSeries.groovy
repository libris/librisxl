import groovy.transform.Memoized

def where = """
    data#>>'{@graph,1,supplementTo}' IS NOT NULL
    AND collection = 'bib'
    AND deleted = 'false'
"""

selectBySqlWhere(where) { bib ->
    def (record, thing) = bib.graph
    
    thing.supplementTo?.each { Map s ->
        if (isTidningSerialReference(s)) {
            incrementStats('supplementTo', s)
            incrementStats('supplementTo shape', s.keySet())
        }
        
    }
}

boolean isTidningSerialReference(Map supplementTo) {
    def controlNumbers = getAtPath(supplementTo, ['describedBy', '*', 'controlNumber'], [])
    def result = controlNumbers.any{ isTidningSerialReference(it) }
    if (controlNumbers.size() > 1) {
        incrementStats('multiple controlnumbers', supplementTo)
    }
    return result
}

@Memoized
boolean isTidningSerialReference(String controlNumber) {
    isTidningSerial(loadThing(controlNumberToId(controlNumber)))
}

static boolean isTidningSerial(Map thing) {
    thing.issuanceType == 'Serial' && getAtPath(thing, ['instanceOf', 'genreForm', '*', '@id'], [])
            .any { it == 'https://id.kb.se/term/saogf/Dagstidningar' || it == 'https://id.kb.se/term/saogf/Tidskrifter' }
}

static def controlNumberToId(String controlNumber) {
    def isXlId = controlNumber.size() > 14
    isXlId
        ? controlNumber
        : 'http://libris.kb.se/resource/bib/' + controlNumber
}

// --------------------------------------------------------------------------------------

Map loadThing(def id) {
    def thing = [:]
    selectByIds([id]) { t ->
        thing = t.graph[1]
    }
    return thing
}

static Object getAtPath(item, Iterable path, defaultTo = null) {
    if(!item) {
        return defaultTo
    }

    for (int i = 0 ; i < path.size(); i++) {
        def p = path[i]
        if (p == '*' && item instanceof Collection) {
            return item.collect { getAtPath(it, path.drop(i + 1), []) }.flatten()
        }
        else if (item[p] != null) {
            item = item[p]
        } else {
            return defaultTo
        }
    }
    return item
}