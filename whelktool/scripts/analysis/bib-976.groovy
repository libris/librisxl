import whelk.util.DocumentUtil
import whelk.util.Statistics

report = getReportWriter("report.txt")
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
    if(bib976) {
        def bib81 = sab(work)
        def (in81, notIn81) = bib976.split { bib81.contains(it['marc:bib976-a'] ?: '')  }

        in81.each {
            s.increment('bib976 in bib81', it)
        }

        notIn81.each{
            s.increment('bib976 not in bib81', it)
            report.println("${bib.doc.getURI()} $notIn81")
        }

    }
}

List sab(work) {
    asList(work['classification']).findAll{ it['inScheme'] ?: '' == 'kssb' }.collect{ it['code'] }
}

def asList(x) {
    (x ?: []).with {it instanceof List ? it : [it] }
}