/**
 * Fix broken subject links.
 *
 * See LXL-3196 for more information.
 */

import groovy.transform.Memoized
import whelk.util.Statistics
import whelk.util.DocumentUtil

class Script {
    static Statistics s = new Statistics().printOnShutdown()
    static PrintWriter fixed
    static PrintWriter notFixed
    static PrintWriter bDeleted
}

Script.fixed = getReportWriter("fixed.txt")
Script.notFixed = getReportWriter("could-not-fix.txt")
Script.bDeleted = getReportWriter("b-deleted.txt")

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

    boolean changed = DocumentUtil.traverse(work['subject']) { subject, List path ->
        if (path.size() != 1 || !subject['@id'] || !is404(subject['@id'])) {
            return DocumentUtil.NOP
        }

        if (subject['@id'] ==~ /_:b\d+/)  {
            // e.g. "@id": "_:b43". Created by failed conversion, see LXL-3283.
            Script.bDeleted.println("${bib.doc.shortId} ${subject['@id']}")
            return new DocumentUtil.Remove()
        }

        String fixed = encodeParens(subject['@id'])
        if (!is404(fixed)) {
            Script.fixed.println("${bib.doc.shortId} ${subject['@id']} --> $fixed")
            subject['@id'] = fixed
            return new DocumentUtil.Replace(subject)
        }
        else {
            Script.notFixed.println("${bib.doc.shortId} ${subject['@id']}")
        }

        return DocumentUtil.NOP
    }

    if (changed) {
        bib.scheduleSave()
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

String encodeParens(String id) {
    id.replace("(", "%28").replace(")", "%29")
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