/**
 * Remove broken GF only containing code
 *
 * See LXL-1628 for more information.
 */

import whelk.util.DocumentUtil
import whelk.util.Statistics

class Script {
    static PrintWriter report
    static Statistics s = new Statistics().printOnShutdown()
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

    if (!work || !work['genreForm']) {
        return
    }

    List gf = work['genreForm']

    if(gf.removeAll { broken(it) }) {
        if (gf.isEmpty()) {
            work.remove('genreForm')
        }

        bib.scheduleSave()
        Script.report.println(bib.doc.shortId)
    }
}

boolean broken(Map gf) {
    if (gf.size() == 1 && gf['code']) {
        Script.s.increment('code', gf['code'])
        return true
    }
    return false
}

Map getWork(def bib) {
    def (record, thing, work) = bib.graph
    if(thing && thing['instanceOf'] && isInstanceOf(thing['instanceOf'], 'Work')) {
        return thing['instanceOf']
    }
    else if (work && isInstanceOf(work, 'Work')) {
        return work
    }
    return null
}