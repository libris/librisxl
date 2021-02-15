String where = """collection = 'bib'
  AND data#>'{@graph,1,instanceOf,subject}' @> '[{"@type": "Work"}]'"""

selectBySqlWhere(where) { data ->
    List subjects = data.graph[1]['instanceOf']['subject']

    boolean modified

    data.graph[1]['instanceOf']['subject'] = subjects.collect { subject ->
        if (subject['@type'] == 'Work' && !subject.containsKey('hasTitle') && subject.containsKey('contribution')) {
            List contribution = subject['contribution']
            // Assuming only one contribution
            Map agent = contribution[0]['agent']

            if (agent) {
                if (isLocalOrganization(agent)) {
                    if (subject['inScheme'])
                        agent['inScheme'] = subject['inScheme']
                    modified = true
                    return agent
                }
                else if (isLinkedOrganization(agent)) {
                    modified = true
                    return agent
                }
            }
        }
        return subject
    }

    if (modified)
        data.scheduleSave()
}

boolean isLocalOrganization(Map agent) {
    return agent['@type'] == 'Organization'
}

boolean isLinkedOrganization(Map agent) {
    if (agent.containsKey('@id')) {
        boolean isOrganization

        selectByIds([agent['@id']]) {
            isOrganization = it.graph[1]['@type'] == 'Organization'
        }

        return isOrganization
    }
    return false
}