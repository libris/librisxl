/**


 See LXL-3390

 */

import groovy.transform.Memoized
import whelk.util.Statistics

class Script {
    static Statistics s = new Statistics()
    static PrintWriter modified
    static PrintWriter errors
}

Script.modified = getReportWriter("modified.txt")
Script.errors = getReportWriter("errors.txt")
Script.s = new Statistics().printOnShutdown()

selectByCollection('bib') { bib ->
    try {
        process(bib)
    }
    catch(Exception e) {
        Script.errors.println("${auth.doc.shortId} $e")
        e.printStackTrace(Script.errors)
    }
}

void process(bib) {
    Map thing = bib.graph[1]

    if (!thing['instanceOf'] || !thing['instanceOf']['subject']) {
        return
    }

    asList(thing['instanceOf']['subject']).each { subject ->
        if (subject instanceof Map && subject['inScheme'] && !subject['inScheme']['@id']) {
            Map inScheme = subject['inScheme']
            Script.modified.println("${bib.doc.shortId} ${inScheme['inScheme']}")

            List sameAs = asList(inScheme['sameAs'])
            if (sameAs) {
                if (sameAs.size() != 1) {
                    Script.s.increment('sameAs - multiple', inScheme)
                }
                else {
                    String link = sameAs.first()['@id']
                    Script.s.increment(is404(link) ? 'sameAs - 404' : 'sameAs - 200', inScheme)
                }
            }
            else {
                Script.s.increment('no sameAs', inScheme)
            }
        }
    }

    //auth.scheduleSave()
}

List asList(o) {
    (o instanceof List) ? (List) o : (o ? [o] : [])
}



@Memoized
boolean is404(String link) {
    boolean result = true
    selectByIds([link]) {
        result = false
    }
    return result
}