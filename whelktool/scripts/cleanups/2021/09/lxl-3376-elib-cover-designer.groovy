def where = """
  collection = 'bib' 
  AND data#>>'{@graph, 1, instanceOf, summary}' like '%Omslagsforgivare:%[Elib]%'
  AND deleted = false
  """

def COV = 'https://id.kb.se/relator/coverDesigner'

selectBySqlWhere(where) { bib ->
    def names = asList(bib.graph[1]['instanceOf']['summary'])
            .findResults { it['label']}
            .findResults { String s -> s.find(/(?:Omslagsformgivare:)([^\[]+)/) }
            .collect { it.substring("Omslagsformgivare:".size()) }
            .collect { it.trim() }

    List workContribution = bib.graph[1]['instanceOf']['contribution']
    def coverDesigners = workContribution
            .findAll { it['role'] == COV || "${it.agent.givenName} ${it.agent.familyName}" in names}

    if (!coverDesigners) {
        println(bib.doc.shortId)
        return
    }
    
    workContribution.removeAll(coverDesigners)

    coverDesigners.each { it['role'] == COV }

    bib.graph[1]['contribution'] = (bib.graph[1]['contribution'] ?: []) + coverDesigners
    
    bib.scheduleSave()
}



private List asList(Object o) {
    if (o == null)
        return []
    if (o instanceof List)
        return o
    return [o]
}