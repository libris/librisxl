import groovy.transform.Memoized
import whelk.util.DocumentUtil

PrintWriter report = getReportWriter("report.txt")

def ids = new File(System.getProperty('ids'))
        .readLines()
        .collect { it.split('\t').collect { it.trim()} }
        .flatten()

selectByIds(ids) { bib -> 
    def langs = [
            [1, 'instanceOf', 'language', 0, '@id'],
            [1, 'instanceOf', 'translationOf', 0, 'language', 0, '@id']
    ].collect {
        langName(getPathSafe(it, '')).toLowerCase() 
    }
    
    boolean changed = DocumentUtil.findKey(bib.graph, "mainTitle") { String value, path ->
        for (lang in langs) {
            String r = value.replaceAll(/(?i)\s+\(\(?\s*${lang}\s*\)\)?\s+$/, '')
            if (value != r) {
                report.println("$value -> $r")
                return new DocumentUtil.Replace(r)
            }
        }
        return DocumentUtil.NOP
    }

    if (changed) {
        bib.scheduleSave()
    }
}

private Object getPathSafe(item, path, defaultTo = null) {
    if (!item) {
        return defaultTo
    }

    for (p in path) {
        if (item[p] != null) {
            item = item[p]
        } else {
            return defaultTo
        }
    }
    return item
}

private List asList(Object o) {
    if (o == null)
        return []
    if (o instanceof List)
        return o
    return [o]
}

@Memoized
private String langName(def id) {
    getPathSafe(loadThing(id), ['prefLabel', 'sv'], "NOT FOUND")
}

private Map loadThing(def id) {
    def thing = [:]
    selectByIds([id]) { t ->
        thing = t.graph[1]
    }
    return thing
}