def scriptId = "https://libris.kb.se/sys/globalchanges/2023/05/lxl-2512-move-contribution-by-relator-domain/script.groovy"

selectBySqlWhere("collection = 'bib' and deleted = false") { bib ->
    def instance = bib.graph[1]
    def work = instance.instanceOf

    if (!work || work['@id']) return

    def newestToOldestVersion = bib.getVersions().reverse()
    def versionAfterMovedIdx = newestToOldestVersion.findIndexOf { it.data['@graph'][0]['generationProcess']?['@id'] == scriptId }

    if (versionAfterMovedIdx == -1) return

    def versionBefore = newestToOldestVersion[versionAfterMovedIdx + 1]
    def workContributionBefore = versionBefore.data['@graph'][1]['instanceOf']['contribution']
    def noRole = workContributionBefore?.collect { !it.role }

    if (!noRole) return

    if (!work['contribution']) {
        work['contribution'] = noRole
        bib.scheduleSave()
    } else {
        noRole.each {
            if (!work['contribution'].contains(it)) {
                if (it['@type'] == 'PrimaryContribution') {
                    work['contribution'] = [it] + work['contribution']
                } else {
                    work['contribution'] += it
                }
                bib.scheduleSave()
            }
        }
    }

    println(bib.doc.shortId)
    println(work['contribution'])
}