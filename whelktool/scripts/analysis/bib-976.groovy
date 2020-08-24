import whelk.util.DocumentUtil
import whelk.util.Statistics

class Script {
    static PrintWriter notIn084
    static PrintWriter in084
    static PrintWriter noCode
    static PrintWriter report
    static PrintWriter errors
}
Script.notIn084 = getReportWriter("not-in-084.txt")
Script.in084 = getReportWriter("in-084.txt")
Script.noCode = getReportWriter("no-code.txt")
Script.report = getReportWriter("report.txt")
Script.errors = getReportWriter("errors.txt")

s = new Statistics().printOnShutdown()

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
    def work = bib.graph[1]['instanceOf']

    if(!work) {
        return
    }

    def bib976 = asList(work['marc:hasBib976'])
    if(!bib976) {
        return
    }

    def (code, noCode) = bib976.split { it['marc:bib976-a'] }
    def bib81 = sab(work)

    handleWithSabCode(bib, work, bib81, code)
    handleWithoutSabCode(bib, work, bib81, noCode)
}

void handleWithSabCode(bib, work, bib084, bib976) {

    bib976.each {
        def (in084, notIn084) = bib976.split { x ->
            def code = x['marc:bib976-a']
            bib084.findAll{ it.startsWith((code)) }
        }

        in084.each {
            s.increment('bib976-a', 'in classification')
        }

        notIn084.each {
            s.increment('bib976-a', 'not in classification')
            s.increment('bib976-a not in classification', it)
        }

        if (notIn084) {
            Script.notIn084.println("""
                ${bib.doc.getURI()}
                bib-976: ${notIn084.collect{ "${it['marc:bib976-a']} (${it['marc:bib976-b']})" }}
                classification/kssb: $bib084
            """.stripIndent())
        }

        if (in084) {
            Script.in084.println("""
                ${bib.doc.getURI()}
                bib-976: ${in084.collect{ "${it['marc:bib976-a']} (${it['marc:bib976-b']})" }}
                classification/kssb: $bib084
            """.stripIndent())
        }

        Script.report.println("${bib.doc.shortId} ${handled(in084, notIn084)}")
    }
}

String handled(in084, notIn084) {
    if (!in084 && notIn084) {
        return "ingen"
    }
    if (in084 && !notIn084) {
        return "alla"
    }
    return "delvis"
}

void handleWithoutSabCode(bib, work, bib084, bib976) {
    if (bib976) {
        def creator = bib.graph[0]['descriptionCreator']['@id']
        s.increment('bib976 without code', creator)

        bib976.each {
            def label = it['marc:bib976-b']
            Script.noCode.println("${bib.doc.getURI()} $creator $label")
        }
    }
}

List sab(work) {
    asList(work['classification']).findAll{ it['inScheme'] ?: '' == 'kssb' }.collect{ it['code'] }
}

def asList(x) {
    (x ?: []).with {it instanceof List ? it : [it] }
}