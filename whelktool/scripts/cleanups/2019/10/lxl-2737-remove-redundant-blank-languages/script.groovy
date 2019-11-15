/**
 * Replace blank language nodes with links
 *
 * See LXL-2737 for more info.
 *
 */

import datatool.scripts.linkblanklanguages.LanguageLinker
import datatool.util.Statistics

import java.util.concurrent.ConcurrentLinkedQueue

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
        'samiska (lulesamiska)'           : 'lulesamiska',
        'samiska (nordsamiska)'           : 'nordsamiska',
        'svenska (fornsvenska)'           : 'fornsvenska',
        'tyska (lågtyska)'                : 'lågtyska',
        'tyska (medelhögtyska)'           : 'medelhögtyska',
        'tyska (medellågtyska)'           : 'medellågtyska',

        // https://www.loc.gov/standards/iso639-2/php/code_changes.php
        // ISO 639-2/B code deprecated in favor of ISO 639-2/T code
        'scc'                             : 'srp',
        'scr'                             : 'hrv',

        // Unambiguous Obsolete MARC codes
        // https://www.kb.se/katalogisering/Formathandboken/Sprakkoder/Sprakkoder/
        // https://www.loc.gov/marc/isochange_ann.html
        'cam':'khm',
        'esk':'ypk',
        'eth':'gez',
        'far':'fao',
        'gae':'gla',
        'gag':'glg',
        'gal':'orm',
        'gua':'grn',
        'int':'ina',
        'iri':'gle',
        'kus':'kos',
        'lan':'oci',
        'lap':'smi',
        'max':'glv',
        'mla':'mlg',
        'sao':'smo',
        'sho':'sna',
        'snh':'sin',
        'sso':'sot',
        'tag':'tgl',
        'taj':'tgk',
        'tar':'tat',
        'tru':'chk',
        'tsw':'tsn',
]

linker = buildLanguageMap()

selectByCollection('auth') { auth ->
    try {
        def (record, thing) = auth.graph

        boolean a = linker.linkLanguages(thing, 'associatedLanguage')
        boolean b = linker.linkLanguages(thing, 'language')

        if (a || b) {
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
        def work = getWork(bib)

        if(!work) {
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
    ConcurrentLinkedQueue<Map> languages = new ConcurrentLinkedQueue<>()
    selectByIds(queryIds(q).collect()) { languages.add(it.graph[1]) }
    languages.forEach({l -> linker.addLanguageDefinition(l) } )

    linker.addSubstitutions(substitutions)
    linker.addMapping('grekiska', 'https://id.kb.se/language/gre')
    linker.addMapping('grekiska', 'https://id.kb.se/language/grc')
    linker.addMapping('greek', 'https://id.kb.se/language/gre')
    linker.addMapping('greek', 'https://id.kb.se/language/grc')

    return linker
}

Map getWork(def bib) {
    def (record, thing, work) = bib.graph
    if (isInstanceOf(thing, 'Work')) {
        return thing
    }
    else if (isInstanceOf(work, 'Work')) {
        return work
    }
    return null
}
