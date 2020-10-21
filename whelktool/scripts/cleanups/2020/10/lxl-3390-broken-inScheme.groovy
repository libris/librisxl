/**


 See LXL-3390

 */

import com.sun.org.glassfish.external.statistics.Statistic
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
            Script.modified.println("${bib.doc.shortId} ${subject['inScheme']}")
            Script.s.increment('inScheme', subject['inScheme'])
        }
    }

    //auth.scheduleSave()
}

List asList(o) {
    (o instanceof List) ? (List) o : (o ? [o] : [])
}