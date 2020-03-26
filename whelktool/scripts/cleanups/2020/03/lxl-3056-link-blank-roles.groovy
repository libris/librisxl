import datatool.scripts.GenericLinker
import datatool.util.Statistics

import java.util.concurrent.ConcurrentLinkedQueue

PrintWriter scheduledForUpdate = getReportWriter("scheduled-for-update")

linker = linker('Role', ['code', 'label', 'prefLabelByLang', 'altLabelByLang', 'hiddenLabel'])

linker.addSubstitutions([
        'http://id.loc.gov/vocabulary/relators/edt' : 'edt',
        'http://id.loc.gov/vocabulary/relators/aut' : 'aut',
        'auth': 'aut', // Author
        'ctb' : 'cbt', // Contributor
        'ed'  : 'edt', // Editor
        'il'  : 'ill', // Illustrator
        'publ': 'pbl', // Publisher
        'tr'  : 'trl', // Translator
        'tra' : 'trl', // Translator

        /*
        Verify these by looking at samples
        'resp' : 'rsp', // Respondent
        'comp' : 'com', // Compiler
        'dir   : 'drt'. // Director

        eks

        */
])

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