def where = """
  collection = 'bib' 
  AND data#>>'{@graph, 1, instanceOf, summary}' like '%ormgivare:%[Elib]%'
  AND deleted = false
  """

roles = [
        'Formgivare:' : 'https://id.kb.se/relator/designer',
        'Omslagsformgivare:' : 'https://id.kb.se/relator/coverDesigner'
]

selectBySqlWhere(where) { bib ->
    def designers= asList(bib.graph[1]['instanceOf']['summary'])
            .findResults { it['label']}
            .join(' ')
            .with { parseDesigners(it) }

    println(designers)
    /*
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
     */
}

private Map parseDesigners(String summary) {
    def roleToNames = roles.collectEntries { s, id ->
        def names = summary
                .findAll(/$s[^\[,]+/)
                .collect { it.substring(s.size()) }
                .collect { it.trim() }
        
        [(id) : names]
    }
    
    def nameToRoles = [:]
    roleToNames.each { role, names ->
        names.each { n -> nameToRoles[n] = nameToRoles.getOrDefault(n, []) + [role] }
    } 
    
    return nameToRoles
}

private List asList(Object o) {
    if (o == null)
        return []
    if (o instanceof List)
        return o
    return [o]
}