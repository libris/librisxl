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
        langName(getPathSafe(bib.graph, it, '')).toLowerCase() 
    }
    println (langs)
    
    boolean changed = DocumentUtil.traverse(bib.graph) { value, path ->
        if (path && 'mainTitle' in path && value instanceof String) {
            for (lang in langs) {
                String r = value.replaceAll(/(?i)\s+\(\(?\s*${lang}\s*\)\)?\s+$/, '')
                if (value != r) {
                    report.println("$value -> $r")
                    return new DocumentUtil.Replace(r)
                }
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

@Memoized
private String langName(def id) {
    getPathSafe(loadThing(id), ['prefLabelByLang', 'sv'], "NOT FOUND")
}

private Map loadThing(def id) {
    def thing = [:]
    selectByIds([id]) { t ->
        thing = t.graph[1]
    }
    return thing
}