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
    def (record, thing, work) = bib.graph

    if(!work) {
        return
    }

    if(work['marc:hasBib976']) {
        println ("""
            ${bib.doc.getURI()}
            ${work['marc:hasBib976']}
            """.stripIndent())
    }
}

String hasTitle(thing, work) {
    if (work.hasTitle) {
        isSameTitle(thing, work) ? "match" : "diff"
    }
    else {
        "no"
    }
}