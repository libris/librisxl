import whelk.filter.GenericLinker
import whelk.util.Statistics

import java.util.concurrent.ConcurrentLinkedQueue

PrintWriter scheduledForUpdate = getReportWriter("scheduled-for-update")

linker = linker('Role', ['code', 'label', 'prefLabelByLang', 'altLabelByLang', 'hiddenLabel'])

linker.addSubstitutions([
        'http://id.loc.gov/vocabulary/relators/edt': 'edt',
        'http://id.loc.gov/vocabulary/relators/aut': 'aut',

        'auth' : 'aut', // Author
        'bpl'  : 'pbl', // Publisher
        'ctb'  : 'cbt', // Contributor
        'ed'   : 'edt', // Editor
        'il'   : 'ill', // Illustrator
        'prees': 'pra', // Preses
        'resp' : 'rsp', // Respondent
        'tr'   : 'trl', // Translator
        'tra'  : 'trl', // Translator

        /*
        'oprac': 'edt', // Editor (Polish)  verify

        dir - both Director & Dirigient
        pres - both presenter and preses
        comp - mostly Compiler and a few Composer
        https://libris-qa.kb.se/katalogisering/search/libris?q=%2a&_limit=300&instanceOf.contribution.role.label=comp

        117 eks - all from two records
        https://libris-qa.kb.se/katalogisering/nzbx5d45l277bwtg
        https://libris-qa.kb.se/katalogisering/lw8v4935jzg3tpq7

        */
])
// These are cases that won't be handled by metadata (definitions) improvements that we still want to clean up

selectByCollection('bib') { bib ->
    try {
        def work = getWork(bib)

        if(!work) {
            return
        }

        if (linker.linkAll(work['contribution'] ?: [:], 'role')) {
            scheduledForUpdate.println("${bib.doc.getURI()}")
            bib.scheduleSave()
        }
    }
    catch (Exception e) {
        println "failed ${bib.doc.getURI()} : ${e}"
        e.printStackTrace()
    }
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

GenericLinker linker(String type, List<String> fields) {
    def q = [
            "@type": [type],
            "q"    : ["*"],
            '_sort': ["@id"]
    ]

    GenericLinker linker = new GenericLinker(type, fields, new Statistics().printOnShutdown())
    ConcurrentLinkedQueue<Map> definitions = new ConcurrentLinkedQueue<>()
    selectByIds(queryIds(q).collect()) { definitions.add(it.graph[1]) }
    definitions.forEach({d -> linker.addDefinition(d) } )

    return linker
}