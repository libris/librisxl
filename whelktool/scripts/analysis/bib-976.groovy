import whelk.util.DocumentUtil
import whelk.util.Statistics

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

    def bib976 = work['marc:hasBib976']
    if(bib976) {
        def bib81 = sab(work)
        def notIn81 = bib976.findAll { !bib81.contains(it['marc:bib976-a'] ?: '')  }
        if(notIn81) {
            notIn81.each{ s.increment('bib976 not in bib81', it) }
            println ("${bib.doc.getURI()} $notIn81")
        }

    }
}

List sab(work) {
    asList(work['classification']).findAll{ it['inScheme'] ?: '' == 'kssb' }.collect{ it['code'] }
}

def asList(x) {
    (x ?: []).with {it instanceof List ? it : [it] }
}