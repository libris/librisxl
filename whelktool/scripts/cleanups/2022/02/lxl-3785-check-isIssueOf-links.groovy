import groovy.transform.Memoized

def where = """
    collection = 'bib'
    AND deleted = 'false'
    AND data#>>'{@graph,1,isIssueOf}' IS NOT NULL
"""

selectBySqlWhere(where) { bib ->
    def (record, thing) = bib.graph
    
    def titles = getAtPath(thing, ['hasTitle', '*', 'mainTitle']).collect(this::stripDate)
    
    getAtPath(thing, ['isIssueOf', '*', '@id'], []).each { String id ->
        titles.each { issueTitle ->
            incrementStats(getSerialTitle(id), issueTitle)
        }
    }
}

@Memoized
String getSerialTitle(String id) {
    def thing = loadThing(id)
    def title = getAtPath(thing, ['hasTitle', '*', 'mainTitle'], []).join(' · ')
    def shortId = id.split('/').last()
    return "$title ($shortId)"
}

def stripDate(String title) {
    def PATTERN = /^(.*)\s+\d\d\d\d-\d\d\-\d\d$/

    def stripped = (title =~ PATTERN).with { matches() ? it[0][1] : title }
    return stripped
}

//-----------------------------------------

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

Map loadThing(def id) {
    def thing = [:]
    selectByIds([id]) { t ->
        thing = t.graph[1]
    }
    return thing
}

static List asList(o) {
    return (o instanceof List) ? (List) o : o != null ? [o] : []
}

