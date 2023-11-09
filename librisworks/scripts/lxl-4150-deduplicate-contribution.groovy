def ids = new File(System.getProperty('clusters')).collect { it.split('\t').collect { it.trim() } }.flatten()

selectByIds(ids) { bib ->
    def work = bib.graph[1].instanceOf
    def contribution = work?.contribution

    if (!contribution) return

    def duplicates = contribution.countBy { asList(it.agent) }.findResults { it.value > 1 ? it.key : null }

    duplicates.each { d ->
        def primaryContributionIdx = contribution.findIndexOf { asList(it.agent) == d && it['@type'] == 'PrimaryContribution' }
        def mergeIntoIdx = primaryContributionIdx > -1
                ? primaryContributionIdx
                : contribution.findIndexOf { asList(it.agent) == d }
        def mergeInto = contribution[mergeIntoIdx]

        def roles = contribution.findResults { asList(it.agent) == d ? asList(it.role) : null }.flatten().unique()
        if (roles) mergeInto['role'] = roles

        def idx = 0
        contribution.removeAll {
            def removeIf = asList(it.agent) == d && idx != mergeIntoIdx
            idx += 1
            return removeIf
        }
    }

    if (duplicates) {
        bib.scheduleSave()
    }
}