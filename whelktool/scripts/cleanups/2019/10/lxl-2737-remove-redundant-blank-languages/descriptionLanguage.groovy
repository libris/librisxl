/**
 * Replace blank language nodes with links in descriptionLanguage
 *
 * See LXL-2737 for more info.
 *
 */

import whelk.filter.LanguageLinker
import whelk.util.Statistics

PrintWriter scheduledForUpdate = getReportWriter("scheduled-for-update")

linker = buildLanguageMap()

Closure fix = { post ->
    try {
        def (record, thing) = post.graph

        if (linker.linkLanguages(record, 'descriptionLanguage')) {
            scheduledForUpdate.println("${post.doc.getURI()}")
            post.scheduleSave()
        }
    }
    catch (Exception e) {
        println "failed ${post.doc.getURI()} : ${e}"
        e.printStackTrace()
    }
}

selectByCollection('auth', fix)
selectByCollection('bib', fix)


LanguageLinker buildLanguageMap() {
    def q = [
            "@type": ["Language"],
            "q"    : ["*"],
            '_sort': ["@id"]
    ]

    LanguageLinker linker = new LanguageLinker([], new Statistics().printOnShutdown())
    queryDocs(q).findAll{ it.code == 'swe' || it.code == 'eng' }.each(linker.&addLanguageDefinition)

    return linker
}
