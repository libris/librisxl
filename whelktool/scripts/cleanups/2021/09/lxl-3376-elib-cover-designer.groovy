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
    def nameToRoles = asList(bib.graph[1]['instanceOf']['summary'])
            .findResults { it['label']}
            .join(' ')
            .with { parseDesigners(it) }
        
    List workContribution = bib.graph[1]['instanceOf']['contribution']
    def coverDesigners = workContribution
            .findAll { roles.values().containsAll(it.role) || nameToRoles.containsKey(name(it.agent))}

    if (!coverDesigners) {
        println("Could not handle: ${bib.doc.shortId}")
        return
    }
    
    workContribution.removeAll(coverDesigners)

    coverDesigners.each { it['role'] == nameToRoles[name(it.agent)] }

    bib.graph[1]['contribution'] = (bib.graph[1]['contribution'] ?: []) + coverDesigners
    
    println(bib.graph[1]['contribution'])
    
    bib.scheduleSave()
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

private String name(Map agent) {
    "${agent.givenName} ${agent.familyName}"
}

private List asList(Object o) {
    if (o == null)
        return []
    if (o instanceof List)
        return o
    return [o]
}