/**
 Change inScheme hsv -> ssif for subjects with valid label.

 Background: "Standard för svensk indelning av forskningsämnen" SSIF. was originally created by Högskoleverket (HSV)
 which no longer exists. Some labels have chaged since the original draft.

 ssif-labels-2016.txt contains valid SSIF labels.
 Taken from the excel-file published by UKÄ and cleaned from typos and double spaces
 https://www.uka.se/download/18.7391c377159bc0155b81ef8/1487841861615/forskningsamnen-standard-2011.xlsx

 ssif-labels-map.tsv maps labels found in Libris data to valid SSIF labels.

 See LXL-3272
 */

import whelk.util.Statistics


class Script {
    static PrintWriter modified
    static PrintWriter errors
    static Statistics statistics = new Statistics(5).printOnShutdown()
    static Set ssifLabels
    static Set<String> seenLabels = Collections.synchronizedSet(new HashSet<String>())
    static Map labelMap
}

Script.modified = getReportWriter("modified.txt")
Script.errors = getReportWriter("errors.txt")

Script.ssifLabels = new File(scriptDir, 'ssif-labels-2016.txt')
        .readLines().collect{ it.trim().toLowerCase() } as Set

Script.labelMap = new File(scriptDir, 'ssif-labels-map.tsv').readLines()
        .collectEntries{it.split('\t').with { cols -> [cols[0].trim().toLowerCase(), cols[1].trim()]}}


File ids = new File(scriptDir, 'ids.txt')
ids.exists() ? selectByIds(ids.readLines(), this.&handle) : selectByCollection('bib', this.&handle)

(Script.ssifLabels - Script.seenLabels).each {
    Script.statistics.increment('D) SSIF labels not seen', it)
}

void handle(bib) {
    try {
        Script.statistics.withContext(bib.doc.shortId) {
            process(bib)
        }
    }
    catch(Exception e) {
        Script.errors.println("${bib.doc.shortId} $e")
        e.printStackTrace(Script.errors)
        e.printStackTrace()
    }
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
            changed |= fixHsvSubject(subject)
        }
    }

    if (changed) {
        Script.modified.println(bib.doc.shortId)
        bib.scheduleSave()
    }
}

boolean fixHsvSubject(Map subject) {
    String prefLabel = subject['prefLabel'].trim().toLowerCase()

    if (Script.labelMap.containsKey(prefLabel)) {
        subject['prefLabel'] = Script.labelMap[prefLabel]
        Script.statistics.increment('B) mapped', "$prefLabel -> ${subject['prefLabel']}")
        prefLabel = subject['prefLabel'].trim().toLowerCase()
    }

    if (prefLabel in Script.ssifLabels) {
        Script.statistics.increment('C) hsv -> ssif', prefLabel)
        Script.seenLabels.add(prefLabel)
        subject['inScheme'] = ['@id': 'https://id.kb.se/term/ssif']
        return true
    }
    else {
        Script.statistics.increment('A) not updated', prefLabel)
        return false
    }
}

List asList(o) {
    (o instanceof List) ? (List) o : (o ? [o] : [])
}