PrintWriter unhandled = getReportWriter("unhandled.txt")

def where = """
  collection = 'bib' 
  AND (data#>>'{@graph, 1, instanceOf, summary}' like '%ormgivare:%[Elib]%' OR data#>>'{@graph, 1, summary}' like '%ormgivare:%[Elib]%')
  AND deleted = false
  """

roles = [
        'Formgivare:' : 'https://id.kb.se/relator/designer',
        'Omslagsformgivare:' : 'https://id.kb.se/relator/coverDesigner'
]

selectBySqlWhere(where) { bib ->
    def summary = asList(bib.graph[1]['instanceOf']['summary']) + asList(bib.graph[1]['summary'])
    def nameToRoles = summary
            .findResults { it['label']}
            .join(' ')
            .with { parseDesigners(it) }
        
    List workContribution = bib.graph[1]['instanceOf']['contribution']
    if (workContribution.removeAll { !it.agent }) {
        bib.scheduleSave()
    }
    
    def coverDesigners = workContribution
            .findAll { (it.role && roles.values().containsAll(it.role)) || nameToRoles.containsKey(name(it.agent))}

    if (!coverDesigners) {
        unhandled.println("${bib.doc.shortId} c:$workContribution d:$nameToRoles")
        return
    }
    
    workContribution.removeAll(coverDesigners)
    
    coverDesigners.each { it['role'] = nameToRoles[name(it.agent)].collect { ['@id' : it] } }

    bib.graph[1]['contribution'] = (bib.graph[1]['contribution'] ?: []) + coverDesigners
        
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