import whelk.util.DocumentUtil
import whelk.util.Statistics

Statistics s = new Statistics().printOnShutdown()

selectByCollection('bib') { bib ->
    def work = getWork(bib)

    if(!work) {
        return
    }

    if(work['marc:languageNote'] == "marc:ItemIsOrIncludesATranslation"
            && noTranslator(work.contribution ?: [])
            && (bib.graph[1].responsibilityStatement ?: "").contains('övers')
    ) {
        println ("""
            ${bib.doc.getURI()}
            ${work.contribution}
            ${bib.graph[1].responsibilityStatement}

            """.stripIndent())
        s.increment('tot', 'tot')
    }
}

boolean noTranslator(def contribution) {
    boolean found = false
    DocumentUtil.findKey(contribution, '@id') { value, path ->
        if (value == 'https://id.kb.se/relator/translator') {
            found = true
        }
        DocumentUtil.NOP
    }

    return !found
}


Map getWork(def bib) {
    def (record, thing, work) = bib.graph
    if (thing && isInstanceOf(thing, 'Work')) {
        return thing
    }
    else if (work && isInstanceOf(work, 'Work')) {
        return work
    }
    return null
}