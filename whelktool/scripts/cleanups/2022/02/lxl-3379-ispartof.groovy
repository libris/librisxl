/* 

*/

import whelk.util.Unicode

def where = """
    collection = 'bib'
    AND deleted = 'false'
    AND data#>>'{@graph,1,issuanceType}' = 'ComponentPart'
    AND data#>>'{@graph,1,isPartOf}' IS NOT NULL
"""

OK_PROPS = ['@type', 'identifiedBy', 'hasTitle', 'describedBy'] as Set

linked = getReportWriter("linked.tsv")
notFound = getReportWriter("not-found.tsv")
unhandled = getReportWriter("unhandled.tsv")
badTitle = getReportWriter("bad-title.tsv")
badIdentifiedBy = getReportWriter("bad-identified-by.tsv")
badControlNumber = getReportWriter("bad-controlnumber.tsv")
//variant = getReportWriter("variant.tsv")

selectBySqlWhere(where) { bib ->
    def (record, thing) = bib.graph

    if (!thing.isPartOf || thing.issuanceType != 'ComponentPart') {
        return
    }
    
    boolean modified = false
    def isPartOf = asList(thing.isPartOf)

    isPartOf.each { Map i ->
        if ((i.keySet() - OK_PROPS).isEmpty()) {
            String id = getLink(bib.doc.shortId, i)
            if (id) {
                i.clear()
                i['@id'] = id
                modified = true
                linked = getReportWriter("linked.tsv")
                linked.println("$bib.doc.shortId\t${isPartOf}")
            }
        }
        else {
            incrementStats('isPartOf unhandled shape', i.keySet())
        }
        
    }
    
    if (modified) {
        thing.isPartOf = isPartOf
        bib.scheduleSave()
    }
}

String getLink(String bibId, Map isPartOf) {
    List controlNumbers = getAtPath(isPartOf, ['describedBy', '*', 'controlNumber'], [])
    if (controlNumbers.size() > 1) {
        incrementStats('multiple controlnumbers', isPartOf)
        return null
    }
    if (controlNumbers.size() == 1) {
        def thing = loadThing(controlNumberToId(controlNumbers.first()))
        if (!thing) {
            badControlNumber.println("$bibId\t${controlNumbers}\t${isPartOf}")
            return null
        }
        return verifyTarget(bibId, isPartOf, thing) 
                ? thing.'@id'
                : null
    }
    else if (asList(isPartOf.identifiedBy).size() == 1) {
        def thing = loadThingByIdentifier(asList(isPartOf.identifiedBy).first())
        return thing && verifyTarget(bibId, isPartOf, thing)
                ? thing.'@id'
                : null
    }
    else {
        //incrementStats('unhandledidentifiedBy', isPartOf.identifiedBy ?: '')
        unhandled.println("$bibId\t${isPartOf}")
        return null
    }
}

boolean verifyTarget(String bibId, Map isPartOf, Map targetThing) {
    if (!verifyTitle(bibId, isPartOf, targetThing)) {
        return false
    }
        
    if (isPartOf.identifiedBy && asSet(isPartOf.identifiedBy) != asSet(targetThing.identifiedBy)) {
        incrementStats('bad identifiedBy', isPartOf)
        badIdentifiedBy.println("$bibId\t${isPartOf.identifiedBy}\t${targetThing.identifiedBy}")
        return false
    }
}

static def controlNumberToId(String controlNumber) {
    def isXlId = controlNumber.size() > 14
    isXlId
            ? controlNumber
            : 'http://libris.kb.se/resource/bib/' + controlNumber
}

Map loadThing(def id) {
    def thing = [:]
    selectByIds([id]) { t ->
        thing = t.graph[1]
    }
    return thing
}


Map loadThingByIdentifier(Map identifier) {
    def q = [
            'identifiedBy.value' : [identifier.value],
            'identifiedBy.@type' : [identifier.'@type'],
    ]

    def ids = queryIds(q).collect()
    if (ids.size() == 1) {
        return loadThing(ids.first())
    }
    else {
        incrementStats('found', "${ids.size()} $identifier ")
        return [:]
    }
} 



boolean verifyTitle(String bibId, Map isPartOf, Map targetThing) {
    def titles = { Map thing ->
        getAtPath(thing, ['hasTitle', '*', 'mainTitle'], []).collect { String title -> Unicode.trim(title.toLowerCase()) }
    }

    def referenceTitles = titles(isPartOf)

    if (!referenceTitles) {
        return true
    }

    if (titles(targetThing).intersect(referenceTitles)) {
        return true
    }
    else {
        //badTitle.println("$bibId\t${referenceTitles}\t${titles(targetThing)}")
        incrementStats('bad title', "${referenceTitles}".toString())
        return false
    }
}

// --------------------------------------------------------------------------------------

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

static List asList(o) {
    return (o instanceof List) ? (List) o : o != null ? [o] : []
}

static Set asSet(Object o) {
    asList(o) as Set
}