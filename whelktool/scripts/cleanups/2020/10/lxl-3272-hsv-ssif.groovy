/**


 See LXL-3272
 */

import whelk.util.Statistics


class Script {
    static PrintWriter modified
    static PrintWriter errors
    static Statistics statistics = new Statistics(5).printOnShutdown()
}

Script.modified = getReportWriter("modified.txt")
Script.errors = getReportWriter("errors.txt")

selectByCollection('bib') { bib ->
    try {
        process(bib)
    }
    catch(Exception e) {
        Script.errors.println("${bib.doc.shortId} $e")
        e.printStackTrace(Script.errors)
    }
}

void process(bib) {
    def thing = bib.graph[1]
    if(!(thing['instanceOf'] && thing['instanceOf']['subject'])) {
        return
    }

    def subjects = asList(thing['instanceOf']['subject'])

    subjects.each { Map subject ->
        if (subject['inScheme'] && subject['inScheme']['code'] == 'hsv') {
            Script.statistics.increment('hsv', subject['prefLabel'], bib.doc.shortId)
        }
    }
}

List asList(o) {
    (o instanceof List) ? (List) o : (o ? [o] : [])
}