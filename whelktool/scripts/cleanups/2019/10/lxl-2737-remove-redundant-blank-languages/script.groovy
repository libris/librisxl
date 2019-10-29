import datatool.scripts.linkblanklanguages.LanguageLinker
import datatool.util.Statistics

/*
 * Replace blank language nodes with links
 *
 * See LXL-2737 for more info.
 *
 */

OBSOLETE_CODES = ['9ss', '9sl']

PrintWriter scheduledForUpdate = getReportWriter("scheduled-for-update")

// These are cases that won't be handled by metadata (definitions) improvements that we still want to clean up
substitutions = [
        'jap'                             : 'jpn',
        'mongoliskt språk'                : 'mongoliska språk',
        'mongoliska'                      : 'mongoliska språk',
        'mongoliska språket'              : 'mongoliska språk',
        'latviešu val'                    : 'lettiska',
        'latviešu valodā'                 : 'lettiska',
        'suomi'                           : 'finska',
        'česky'                           : 'tjeckiska',
        'ruotsi'                          : 'svenska',

        'svenska (fornsvenska)'           : 'fornsvenska',
        'grekiska (nygrekiska)'           : 'nygrekiska',
        'franska (fornfranska)'           : 'fornfranska',
        'tyska (medelhögtyska)'           : 'medelhögtyska',
        'engelska (medelengelska)'        : 'medelengelska',
        'engelska (fornengelska)'         : 'fornengelska',
        'tyska (lågtyska)'                : 'lågtyska',
        'nederländska (medelnederländska)': 'medelnederländska',
        'norska (nynorsk)'                : 'nynorsk',
        'franska (medelfranska)'          : 'medelfranska',
        'tyska (medellågtyska)'           : 'medellågtyska',
        'arabiska (judearabiska)'         : 'judearabiska',
        'samiska (nordsamiska)'           : 'nordsamiska',
        'samiska (lulesamiska)'           : 'lulesamiska'
]

linker = buildLanguageMap()

selectByCollection('bib') { bib ->
    try {
        def (record, thing, work) = bib.graph
        if (!((String) work['@id']).endsWith('#work')) {
            return
        }

        if (linker.linkLanguages(work)) {
            scheduledForUpdate.println("${bib.doc.getURI()}")
            bib.scheduleSave()
        }
    }
    catch (Exception e) {
        println "failed ${bib.doc.getURI()} : ${e}"
        e.printStackTrace()
    }
}

LanguageLinker buildLanguageMap() {
    def q = [
            "@type": ["Language"],
            "q"    : ["*"],
            '_sort': ["@id"]
    ]

    LanguageLinker linker = new LanguageLinker(OBSOLETE_CODES, new Statistics().printOnShutdown())
    queryDocs(q).each(linker.&addLanguageDefinition)

    linker.addSubstitutions(substitutions)
    linker.addMapping('grekiska', 'https://id.kb.se/language/gre')
    linker.addMapping('grekiska', 'https://id.kb.se/language/grc')

    return linker
}
