/**
 * Remove language appearing as a substring of work main title if already described in the language property.
 * E.g. "Pippi Långstrump (Svenska)" --> "Pippi Långstrump" when work.language = [{'@id': 'https://id.kb.se/language/swe'}]
 */

import groovy.transform.Memoized
import whelk.util.DocumentUtil

PrintWriter report = getReportWriter("report.txt")

def ids = new File(System.getProperty('clusters'))
        .readLines()
        .collect { it.split('\t').collect { it.trim()} }
        .flatten()

selectByIds(ids) { bib -> 
    def langs = [
            [1, 'instanceOf', 'language', 0, '@id'],
            [1, 'instanceOf', 'translationOf', 0, 'language', 0, '@id']
    ].collect {
        langName(getAtPath(bib.graph, it, '')).toLowerCase()
    }

    def work = bib.graph[1].instanceOf

    if (!work || work['@id']) return
    
    boolean changed = DocumentUtil.traverse(work) { value, path ->
        if (path && 'mainTitle' in path && value instanceof String) {
            for (lang in langs) {
                String r = value.replaceAll(/(?i)\s*\(\(?\s*${lang}\s*\)\)?\s*$/, '')
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

@Memoized
private String langName(def id) {
    getAtPath(loadThing(id), ['prefLabelByLang', 'sv'], "NOT FOUND")
}

private Map loadThing(def id) {
    def thing = [:]
    selectByIds([id]) { t ->
        thing = t.graph[1]
    }
    return thing
}