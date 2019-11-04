/**
 * Replace blank language nodes with links
 *
 * See LXL-2737 for more info.
 *
 */

import datatool.scripts.linkblanklanguages.LanguageLinker
import datatool.util.Statistics

OBSOLETE_CODES = ['9ss', '9sl']

PrintWriter scheduledForUpdate = getReportWriter("scheduled-for-update")

// These are cases that won't be handled by metadata (definitions) improvements that we still want to clean up
substitutions = [
        'catalán'                         : 'katalanska',
        'dansk'                           : 'danska',
        'engl'                            : 'engelska',
        'fornkyrkoslaviska'               : 'fornkyrkslaviska',
        'francais'                        : 'franska',
        'inglés'                          : 'engelska',
        'jap'                             : 'jpn',
        'kroat'                           : 'kroatiska',
        'latviešu val'                    : 'lettiska',
        'latviešu valodā'                 : 'lettiska',
        'mongoliska språket'              : 'mongoliska språk',
        'mongoliska'                      : 'mongoliska språk',
        'mongoliskt språk'                : 'mongoliska språk',
        'ruotsi'                          : 'svenska',
        'schwed'                          : 'svenska',
        'suomi'                           : 'finska',
        'svensk'                          : 'svenska',
        'tigriniska'                      : 'tigrinska',
        'á íslensku'                      : 'isländska',
        'česky'                           : 'tjeckiska',

        'arabiska (judearabiska)'         : 'judearabiska',
        'engelska (fornengelska)'         : 'fornengelska',
        'engelska (medelengelska)'        : 'medelengelska',
        'franska (fornfranska)'           : 'fornfranska',
        'franska (medelfranska)'          : 'medelfranska',
        'french (middle french)'          : 'medelfranska',
        'grekiska (nygrekiska)'           : 'nygrekiska',
        'nederländska (medelnederländska)': 'medelnederländska',
        'norska (nynorsk)'                : 'nynorska',
        'norska (nynorska)'               : 'nynorska',
        'samiska (lulesamiska)'           : 'lulesamiska'
        'samiska (nordsamiska)'           : 'nordsamiska',
        'svenska (fornsvenska)'           : 'fornsvenska',
        'tyska (lågtyska)'                : 'lågtyska',
        'tyska (medelhögtyska)'           : 'medelhögtyska',
        'tyska (medellågtyska)'           : 'medellågtyska',
]

linker = buildLanguageMap()

selectByCollection('auth') { auth ->
    try {
        def (record, thing) = auth.graph
        if (linker.linkLanguages(thing, 'associatedLanguage') || linker.linkLanguages(thing, 'language')) {
            scheduledForUpdate.println("${auth.doc.getURI()}")
            auth.scheduleSave()
        }
    }
    catch (Exception e) {
        println "failed ${auth.doc.getURI()} : ${e}"
        e.printStackTrace()
    }
}

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
    linker.addMapping('greek', 'https://id.kb.se/language/grc')
    linker.addMapping('greek', 'https://id.kb.se/language/grc')

    return linker
}
