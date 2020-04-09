/**
 This script tries to link blank contribution.role nodes using
    - Role labels in definitions
    - clean-up mappings based on most common blank nodes in Libris data

 See LXL-3056 for more info
*/

import whelk.filter.BlankNodeLinker
import whelk.util.Statistics

import java.util.concurrent.ConcurrentLinkedQueue

PrintWriter scheduledForUpdate = getReportWriter("scheduled-for-update")

linker = linker('Role', ['code', 'label', 'prefLabelByLang', 'altLabelByLang', 'hiddenLabel'])

// These are cases that won't be handled by metadata (definitions) improvements that we still want to clean up
linker.addDeletions([
        'funktionskod',
        'funktionskod (läggs manuellt) oftast pbl',
        'kssb/8'
])

linker.addSubstitutions([
        'http://id.loc.gov/vocabulary/relators/edt': 'edt',
        'http://id.loc.gov/vocabulary/relators/aut': 'aut',

        'aaut'                                     : 'aut',
        'a ut'                                     : 'aut',
        'artiste'                                  : 'art',
        'artists'                                  : 'art',
        'attributed anme'                          : 'att',
        'auhtor'                                   : 'aut',
        'auhor'                                    : 'aut',
        'authors'                                  : 'aut',
        'autt'                                     : 'aut',
        'autr'                                     : 'aut',
        'coeditor'                                 : 'edt',
        'compositeur'                              : 'cmp',
        'ditor'                                    : 'edt',
        'e aut'                                    : 'aut',
        'eauthor'                                  : 'aut',
        'éditeur intellectuel'                     : 'edt',
        'editor literari'                          : 'edt',
        'editior'                                  : 'edt',
        'editora'                                  : 'edt',
        'editor of compilaton'                     : 'edc',
        'editor of compliation'                    : 'edc',
        'editor de la compilación'                 : 'edc',
        'editorial board member'                   : 'edt',
        'edtitor'                                  : 'edt',
        'esitt©þj©þ'                               : 'esittäjä',
        'f©œrfattare'                              : 'aut',
        'illustrations / graphisme'                : 'ill',
        'komponistin'                              : 'cmp',
        'k©þ©þnt©þj©þ'                             : 'kääntäjä',
        'medarbetare'                              : 'oth',
        'pht (expression)'                         : 'pht',
        'photographer (expression)'                : 'pht',
        'redakt©œr'                                : 'edt',
        'ritstjórn'                                : 'edt',
        'sarjakuvantekij©þ'                        : 'sarjakuvantekijä',
        'sonstige person, familie und körperschaft': 'oth',
        'sponsorin'                                : 'spn',
        'teksti autor'                             : 'aut',
        'translation'                              : 'trl',
        'translators'                              : 'trl',
        'unspecifiedcontributor'                    : 'unspecified contributor',
        'valokuvaaja (ekspressio)'                 : 'pht',  // Finnish: Photographer (expression)
        'valokuvaaja (ekpressio)'                  : 'pht',  // Finnish: Photographer (expression)
        '(verfasser)'                              : 'aut',


        '0th'     : 'oth', // Other
        '4aut'    : 'aut', // Author
        '4 aut'   : 'aut', // Author
        '4 edt'   : 'edt', // Editor
        '4dt'     : 'edt', // Editor
        '4edt'    : 'edt', // Editor
        '4oth'    : 'oth', // Other
        'auth'    : 'aut', // Author
        'au'      : 'aut', // Author
        'bpl'     : 'pbl', // Publisher
        'ctb'     : 'cbt', // Contributor
        '[(ed.)]' : 'edt', // Editor
        '(ed.)'   : 'edt', // Editor
        '(edt)'   : 'edt', // Editor
        'dt'      : 'edt', // Editor
        'ed'      : 'edt', // Editor
        'eth'     : 'edt', // Editor
        'ed'      : 'edt', // Editor
        'éd'      : 'edt', // Editor
        'edit'    : 'edt', // Editor
        'editors' : 'edt', // Editor
        'edr'     : 'edt', // Editor
        'eds'     : 'edt', // Editor
        'edtor'   : 'edt', // Editor
        'edt3'    : 'edt', // Editor
        'edt6'    : 'edt', // Editor
        'edt7'    : 'edt', // Editor
        'etd'     : 'edt', // Editor
        'edty'    : 'edt', // Editor
        'etdh'    : 'edt', // Editor
        'e dt'    : 'edt', // Editor
        'ford'    : 'trl', // Translator (Hungarian: fordító)
        'foto'    : 'pht', // Photo
        'il'      : 'ill', // Illustrator
        'joint ed': 'edt', // Joint editor -> Editor
        'oaut'    : 'aut', // Author
        'oht'     : 'oth', // Other
        'o th'    : 'oth', // Other
        'prees'   : 'pra', // Preses
        'p bl'    : 'pbl', // Publisher
        'pdb'     : 'pbd', // Publishing director
        'plb'     : 'pbl', // Publisher
        'pub'     : 'pbl', // Publisher
        'resp'    : 'rsp', // Respondent
        't rl'    : 'trl', // Translator
        'tr'      : 'trl', // Translator
        'tra'     : 'trl', // Translator
        'trk'     : 'trl', // Translator
        'trsl'    : 'trl', // Translator
        'wyd'     : 'pbl', // Publisher (Polish: wydawca)

        /*
        //
        760 arkivbildare
        76 sarjakuvantekijä - serieskapare http://finto.fi/mts/fi/page/m1301
        'oprac' // Polish, lit. "Bearbetning" --> Adapter / Editor?
        dir - both Director & Dirigient --> use film director for moving image?
        pres - both presenter and preses
        comp - mostly Compiler and a few Composer
        https://libris-qa.kb.se/katalogisering/search/libris?q=%2a&_limit=300&instanceOf.contribution.role.label=comp

        117 eks - all from two records
        https://libris-qa.kb.se/katalogisering/nzbx5d45l277bwtg
        https://libris-qa.kb.se/katalogisering/lw8v4935jzg3tpq7
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

BlankNodeLinker linker(String type, List<String> fields) {
    def q = [
            "@type": [type],
            "q"    : ["*"],
            '_sort': ["@id"]
    ]

    BlankNodeLinker linker = new BlankNodeLinker(type, fields, new Statistics().printOnShutdown())
    ConcurrentLinkedQueue<Map> definitions = new ConcurrentLinkedQueue<>()
    selectByIds(queryIds(q).collect()) { definitions.add(it.graph[1]) }
    definitions.forEach({d -> linker.addDefinition(d) } )

    return linker
}