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

            if (agent && isOrganization(agent)) {
                modified = true
                return agent
            }
        }
        return subject
    }

    if (modified)
        data.scheduleSave()
}

boolean isOrganization(Map agent) {
    if (agent.containsKey('@id')) {
        boolean linkedEntityIsOrganization

        selectByIds([agent['@id']]) {
            linkedEntityIsOrganization = it.graph[1]['@type'] == 'Organization'
        }

        return linkedEntityIsOrganization
    }
    return localEntityIsOrganization = agent['@type'] == 'Organization'
}