EXPECTED = [TYPE, 'editionStatement', 'editionStatementRemainder'] as Set

selectBySqlWhere(""" data#>'{@graph,1,otherEdition}' notnull """) { data ->
    def instance = data.graph[1]

    //def stmts = []
    def iter = instance.otherEdition?.iterator()
    for (other in iter) {
        if ((other.keySet() - EXPECTED).size() == 0 &&
            (other.editionStatement || other.editionStatementRemainder)) {
            iter.remove()
            data.scheduleSave()
        }
    }

    if (!instance.otherEdition) {
        instance.remove('otherEdition')
    }

    /* NOTE: we DROP these; determined to be import noise of no value.
    if (stmts) {
        def notes = instance.get('hasNote', [])
        notes << stmts.collect {
            [
                (TYPE): 'Note',
                label: [it.editionStatement, it.editionStatementRemainder].findAll().join(' / ')
            ]
        }
        data.scheduleSave()
    }
    */
}
