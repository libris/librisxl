/**


 See LXL-3272
 */

import whelk.util.Statistics


class Script {
    static PrintWriter modified
    static PrintWriter errors
    static Statistics statistics = new Statistics(5).printOnShutdown()
    static Set ssifLabels
    static Set<String> seenLabels = Collections.synchronizedSet(new HashSet<String>())
}

Script.modified = getReportWriter("modified.txt")
Script.errors = getReportWriter("errors.txt")

Script.ssifLabels = new File(scriptDir, 'ssif-labels-2016.txt')
        .readLines().collect{ it.trim().toLowerCase() } as Set

def c = { bib ->
    try {
        process(bib)
    }
    catch(Exception e) {
        Script.errors.println("${bib.doc.shortId} $e")
        e.printStackTrace(Script.errors)
    }
}

File ids = new File(scriptDir, 'ids.txt')
ids.exists() ? selectByIds(ids.readLines(), c) : selectByCollection('bib', c)

(Script.ssifLabels - Script.seenLabels).each {
    Script.statistics.increment('not seen', it)
}

void process(bib) {
    def thing = bib.graph[1]
    if(!(thing['instanceOf'] && thing['instanceOf']['subject'])) {
        return
    }

    def subjects = asList(thing['instanceOf']['subject'])

    boolean changed = false
    subjects.each { Map subject ->
        if (subject['inScheme'] && subject['inScheme']['code'] == 'hsv') {
            String prefLabel = subject['prefLabel'].trim().toLowerCase()
            if (prefLabel in Script.ssifLabels) {
                Script.statistics.increment('mapped', prefLabel, bib.doc.shortId)
                Script.seenLabels.add(prefLabel)
                subject['inScheme'] = ['@id': 'https://id.kb.se/term/ssif']
                changed = true
            }
            else {
                Script.statistics.increment('not mapped', prefLabel, bib.doc.shortId)
            }
        }
    }

    if (changed) {
        Script.modified.println(bib.doc.shortId)
        bib.scheduleSave()
    }
}

List asList(o) {
    (o instanceof List) ? (List) o : (o ? [o] : [])
}