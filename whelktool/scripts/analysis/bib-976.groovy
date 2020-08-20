import whelk.util.DocumentUtil
import whelk.util.Statistics

class Script {
    static PrintWriter notIn81
    static PrintWriter noCode
}
Script.notIn81 = getReportWriter("not-in-81.txt")
Script.noCode = getReportWriter("no-code.txt")

s = new Statistics().printOnShutdown()

selectByCollection('bib') { bib ->
    try {
        process(bib)
    }
    catch(Exception e) {
        System.err.println(e)
        e.printStackTrace()
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

void handleWithSabCode(bib, work, bib81, bib976) {

    bib976.each {
        def (in81, notIn81) = bib976.split { bib81.contains(it['marc:bib976-a'])  }

        in81.each {
            s.increment('bib976-a', 'in classification')
        }

        notIn81.each {
            s.increment('bib976-a', 'not in classification')
        }

        if (notIn81) {
            Script.notIn81.println("""
                ${bib.doc.getURI()}
                bib-976: $notIn81
                classification/kssb: $bib81
            """.stripIndent())
        }
    }
}

void handleWithoutSabCode(bib, work, bib81, bib976) {
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