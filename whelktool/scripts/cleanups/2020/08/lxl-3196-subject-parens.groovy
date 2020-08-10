/**
 * Fix broken subject links.
 *
 * See LXL-3196 for more information.
 */

import groovy.transform.Memoized
import whelk.util.Statistics

class Script {
    static Statistics s = new Statistics().printOnShutdown()
    static PrintWriter report
}

Script.report = getReportWriter("report.txt")

selectByCollection('bib') { bib ->
    try {
        process(bib)
    }
    catch(Exception e) {
        System.err.println("${bib.doc.shortId} $e")
        e.printStackTrace()
    }
}

void process(bib) {
    Map work = getWork(bib)

    if (!work || !work['subject']) {
        return
    }

    work['subject'].each { subject ->
        if (subject['@id'] && is404(subject['@id'])) {
            Script.report.println("${bib.doc.shortId} ${subject['@id']}")
        }
    }
}

@Memoized
boolean is404(String link) {
    boolean result = true
    selectByIds([link]) {
        result = false
    }
    return result
}

Map getWork(def bib) {
    def (_record, thing, work) = bib.graph
    if(thing && thing['instanceOf'] && thing['instanceOf']['@type']) {
        return thing['instanceOf']
    }
    else if (work) {
        return work
    }
    return null
}