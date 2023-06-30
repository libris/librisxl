package mergeworks.scripts

import groovy.transform.Memoized

import java.util.concurrent.ConcurrentHashMap

PrintWriter matchedAndSpecified = getReportWriter("matched.tsv")
PrintWriter unmatchedSpecifiedAnyway = getReportWriter("mismatched.tsv")
PrintWriter matchedInOtherWork = getReportWriter("matched-in-other-work.tsv")
PrintWriter notSpecifiedMovedToInstance = getReportWriter("not-specified-moved-to-instance.txt")

def where = """
  collection = 'bib'
  AND data#>>'{@graph, 0, identifiedBy}' LIKE '%Elib%'
  AND (data#>>'{@graph, 1, instanceOf, summary}' is not null OR data#>>'{@graph, 1, summary}' is not null)
  AND deleted = false
  """

ROLES = [
        'Formgivare:'       : 'https://id.kb.se/relator/bookDesigner',
        'Omslag:'           : 'https://id.kb.se/relator/coverDesigner',
        'Omslagsformgivare:': 'https://id.kb.se/relator/coverDesigner',
]

OTHER = [['@id': 'https://id.kb.se/relator/unspecifiedContributor']]

Map<String, Set<String>> knownNames = new ConcurrentHashMap(['https://id.kb.se/relator/bookDesigner'     : new ConcurrentHashMap().newKeySet(),
                                                             'https://id.kb.se/relator/coverDesigner': new ConcurrentHashMap().newKeySet()])
Map<String, Set<String>> knownAgents = new ConcurrentHashMap(['https://id.kb.se/relator/bookDesigner'     : new ConcurrentHashMap().newKeySet(),
                                                              'https://id.kb.se/relator/coverDesigner': new ConcurrentHashMap().newKeySet()])
Set<String> handled = new ConcurrentHashMap().newKeySet()

selectBySqlWhere(where) { bib ->
    def id = bib.doc.shortId
    def instance = bib.graph[1]
    def summary = asList(instance['instanceOf']['summary']) + asList(bib.graph[1]['summary'])

    def nameToRoles = summary
            .findResults { it['label'] }
            .join(' ')
            .with { parseRoles(it) }
            .each { name, roles ->
                knownNames.computeIfAbsent(name, f -> []).add(roles)
            }

    List workContribution = instance['instanceOf']['contribution']
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
        if (knownAgents.keySet().intersect(roles)) {
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
    def instance = bib.graph[1]
    List workContribution = instance['instanceOf']['contribution']
    if (!workContribution) {
        return
    }

    workContribution.removeAll { !it.agent }

    workContribution.each { c ->
        if (asList(c.role) == OTHER) {
            def roles = knownAgents[c.agent] ?: knownNames[name(loadIfLink(c.agent))]
            if (roles) {
                def countByRole = roles.countBy { it }.sort { -it.value }
                if (countByRole.size() == 1) {
                    countByRole.find { it.value > 2 }?.with {
                        def role = it.key.find()
                        def count = it.value
                        c['role'] = [['@id': role]]
                        matchedInOtherWork.println([id, c.agent, role, count].join('\t'))
                        bib.scheduleSave()
                    }
                }
            }
        }
    }

    workContribution.removeAll { c ->
        if (asList(c.role) == OTHER) {
            instance['contribution'] = asList(instance['contribution']) + c
            notSpecifiedMovedToInstance.println(id)
            bib.scheduleSave()
            return true
        }
        return false
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