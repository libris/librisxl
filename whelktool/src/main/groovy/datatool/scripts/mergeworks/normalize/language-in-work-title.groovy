package datatool.scripts.mergeworks.normalize

import groovy.transform.Memoized
import whelk.util.DocumentUtil

import static datatool.scripts.mergeworks.Util.getPathSafe

/**
 Example:
 $ ENV=qa && time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -Dclusters="reports/clusters.tsv" -jar build/libs/whelktool.jar --report reports/$ENV-$(date +%Y%m%d-%H%M%S) --dry-run src/main/groovy/datatool/scripts/mergeworks/normalize/language-in-work-title.groovy
 */

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
        langName(getPathSafe(bib.graph, it, '')).toLowerCase() 
    }
    
    boolean changed = DocumentUtil.traverse(bib.graph[1].instanceOf) { value, path ->
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
    getPathSafe(loadThing(id), ['prefLabelByLang', 'sv'], "NOT FOUND")
}

private Map loadThing(def id) {
    def thing = [:]
    selectByIds([id]) { t ->
        thing = t.graph[1]
    }
    return thing
}