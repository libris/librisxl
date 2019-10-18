import datatool.util.LanguageLinker
import datatool.util.LanguageMapper
import datatool.util.Statistics

/*
 * This removes blank language nodes
 *
 * See LXL-2737 for more info.
 *
 */

OBSOLETE_CODES = ['9ss', '9sl']

PrintWriter scheduledForUpdate = getReportWriter("scheduled-for-update")
statistics = getReportWriter("statistics.txt")

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

linker = new LanguageLinker(buildLanguageMap())

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

LanguageMapper buildLanguageMap() {
    def q = [
            "@type": ["Language"],
            "q"    : ["*"],
            '_sort': ["@id"]
    ]

    LanguageMapper mapper = new LanguageMapper(OBSOLETE_CODES, new Statistics().printOnShutdown())
    queryDocs(q).each(mapper.&addLanguageDefinition)

    mapper.addSubstitutions(substitutions)
    mapper.addMapping('grekiska', 'http://id/gre')
    mapper.addMapping('grekiska', 'http://id/grc')

    return mapper
}
