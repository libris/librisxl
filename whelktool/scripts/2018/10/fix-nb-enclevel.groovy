selectBySqlWhere('''
    data#>>'{@graph,0,encodingLevel}' = 'marc:PartialPreliminaryLevel'
    AND data#>>'{@graph,0,bibliography,0,sigel}' = 'NB'
''') { data ->
    record = data.graph[0]
    if (record.encodingLevel == 'marc:PartialPreliminaryLevel'
        && record.bibliography?.get(0)?.sigel == 'NB') {
        record.encodingLevel = 'marc:FullLevel'
        data.scheduleSave(false)
    }
}
