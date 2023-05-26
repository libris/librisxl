import groovy.transform.Memoized

PrintWriter matchedAndSpecified = getReportWriter("matched.tsv")
PrintWriter unmatchedSpecifiedAnyway = getReportWriter("mismatched.tsv")
PrintWriter matchedInOtherWork = getReportWriter("matched-in-other-work.tsv")
PrintWriter unhandled = getReportWriter("unhandled.tsv")

def where = """
  collection = 'bib'
  AND data#>>'{@graph, 0, identifiedBy}' LIKE '%Elib%'
  AND (data#>>'{@graph, 1, instanceOf, summary}' is not null OR data#>>'{@graph, 1, summary}' is not null)
  AND deleted = false
  """

ROLES = [
        'Formgivare:'       : 'https://id.kb.se/relator/designer',
        'Omslag:'           : 'https://id.kb.se/relator/coverDesigner',
        'Omslagsformgivare:': 'https://id.kb.se/relator/coverDesigner',
]

OTHER = [['@id': 'https://id.kb.se/relator/unspecifiedContributor']]

Map<String, List<List<String>>> knownNames = Collections.synchronizedMap(['https://id.kb.se/relator/designer'     : [] as Set,
                                                                          'https://id.kb.se/relator/coverDesigner': [] as Set])
Map<Map, List<List<String>>> knownAgents = Collections.synchronizedMap(['https://id.kb.se/relator/designer'     : [] as Set,
                                                                        'https://id.kb.se/relator/coverDesigner': [] as Set])
Set<String> handled = Collections.synchronizedSet([] as Set)

selectBySqlWhere(where) { bib ->
    def id = bib.doc.shortId
    def summary = asList(bib.graph[1]['instanceOf']['summary']) + asList(bib.graph[1]['summary'])

    def nameToRoles = summary
            .findResults { it['label'] }
            .join(' ')
            .with { parseRoles(it) }
            .each { name, roles ->
                knownNames.computeIfAbsent(name, f -> []).add(roles)
            }

    List workContribution = bib.graph[1]['instanceOf']['contribution']
    if (!workContribution) {
        return
    }

    def modified = workContribution.removeAll { !it.agent }

    Set existingRoles = workContribution.collect { asList(it.role)*.'@id' }.grep().flatten()

    if (existingRoles.contains('https://id.kb.se/relator/unspecifiedContributor') && nameToRoles) {
        workContribution.each { c ->
            if (asList(c.role) == OTHER) {
                def agentName = name(loadIfLink(c.agent))
                def roles = nameToRoles[agentName]
                if (roles) {
                    c['role'] = roles.collect { ['@id': it] }
                    matchedAndSpecified.println([id, c.agent, roles].join('\t'))
                    nameToRoles.remove(agentName)
                    modified = true
                }
            }
        }

        def other = workContribution.findAll { asList(it.role) == OTHER }

        if (nameToRoles.size() == 1 && other.size() == 1) {
            def c = other[0]
            def name = nameToRoles.keySet()[0]
            def roles = nameToRoles[name]
            other[0]['role'] = roles.collect { ['@id': it] }
            other.clear()
            unmatchedSpecifiedAnyway.println([id, c.agent, name, roles].join('\t'))
            modified = true
        }

        if (other.isEmpty()) {
            handled.add(id)
        }
    }


    workContribution.each { c ->
        def roles = asList(c.role)*.'@id'
        if (m.keySet().intersect(roles)) {
            knownAgents.computeIfAbsent(c.agent, f -> []).add(roles)
        }
    }

    if (modified) {
        bib.scheduleSave()
    }
}

selectBySqlWhere("collection = 'bib' AND data#>>'{@graph, 0, identifiedBy}' LIKE '%Elib%' AND deleted = false") { bib ->
    def id = bib.doc.shortId
    if (id in handled) {
        return
    }

    List workContribution = bib.graph[1]['instanceOf']['contribution']
    if (!workContribution) {
        return
    }

    workContribution.removeAll { !it.agent }

    workContribution.each { c ->
        if (asList(c.role) == OTHER) {
            def roles = knownAgents[c.agent] ?: knownNames[name(loadIfLink(c.agent))]
            if (roles) {
                def countByRole = roles.countBy { it }.sort {-it.value }
                c['role'] = countByRole.max { it.value }.key.collect { ['@id': it] }
                matchedInOtherWork.println(
                        [
                                id,
                                c.agent,
                                c['role'],
                                countByRole.collectEntries { k, v -> [k.collect { it.split("/").last() }, v] }
                        ].join('\t')
                )
                bib.scheduleSave()
            }
        }
    }

    def other = workContribution.findAll { asList(it.role) == OTHER }

    if (other) {
        unhandled.println([id, other.collect { it.agent }].join('\t'))
    }
}

private Map parseRoles(String summary) {
    def roleToNames = ROLES.collectEntries { s, id ->
        def names = summary
                .findAll(/$s[^\[,"]+/)
                .collect { it.substring(s.size()) }
                .collect { it.trim() }

        [(id): names]
    }

    def nameToRoles = [:]
    roleToNames.each { role, names ->
        names.each { n -> nameToRoles[n] = nameToRoles.getOrDefault(n, []) + [role] }
    }

    return nameToRoles
}

private String name(Map agent) {
    agent.name ?: "${agent.givenName} ${agent.familyName}"
}

private Map loadIfLink(Map m) {
    m['@id'] ? loadThing(m['@id']) : m
}

@Memoized
private Map loadThing(def id) {
    def thing = [:]
    selectByIds([id]) { t ->
        thing = t.graph[1]
    }
    return thing
}