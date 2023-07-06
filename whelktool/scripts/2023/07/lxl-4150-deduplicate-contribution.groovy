import whelk.util.DocumentUtil

selectBySqlWhere("collection = 'auth' and data#>>'{@graph,1,@type}' = 'Text'") {
    boolean hasDuplicates = it.graph[1].contribution?.countBy { asList(it.agent) }?.any { it.value > 1 }
    if (hasDuplicates) {
        it.scheduleSave()
    }
}

selectByCollection('bib') { bib ->
    def instance = bib.graph[1]

    def needsSave = false

    DocumentUtil.findKey(instance, 'contribution') { contribution, _ ->
        def duplicates = asList(contribution).countBy { asList(it.agent) }.findResults { it.value > 1 ? it.key : null }
        if (duplicates) {
            needsSave = true
            return
        }
    }

    if (needsSave) {
        bib.scheduleSave()
    }
}