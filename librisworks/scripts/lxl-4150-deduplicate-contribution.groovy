def ids = new File(System.getProperty('clusters')).collect { it.split('\t').collect { it.trim() } }.flatten()

selectByIds(ids) { bib ->
    def work = bib.graph[1].instanceOf
    def contribution = work?.contribution

    if (!contribution) return

    def duplicates = contribution.countBy { asList(it.agent) }.findResults { it.value > 1 ? it.key : null }

    duplicates.each { d ->
        def firstIdx = contribution.findIndexOf { asList(it.agent) == d }
        def first = contribution[firstIdx]
        def roles = []

        contribution.removeAll {
            if (asList(it.agent) == d) {
                roles += asList(it.role)
                return true
            }
            return false
        }

        if (roles) first['role'] = roles.unique()

        contribution.add(firstIdx, first)
    }

    if (duplicates) {
        bib.scheduleSave()
    }
}