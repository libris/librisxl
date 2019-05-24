LIBRARY = 'Library'

selectBySqlWhere("""
    deleted = false AND
    collection = 'bib' AND
    data#>>'{@graph,0,bibliography,0,sigel}' LIKE '%,%'
""") { data ->
    def record = data.graph[0]

    record.bibliography.each {
        assert it.keySet() == [TYPE, 'sigel'] as Set
        assert it[TYPE] == LIBRARY
    }

    record.bibliography = record.bibliography.collect {
        it.sigel instanceof List ? it.sigel : [it.sigel]
    }.flatten().collect {
        [(TYPE): LIBRARY, sigel: it]
    }

    data.scheduleSave(loud: false)
}
