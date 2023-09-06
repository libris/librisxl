def where = """
    collection = 'bib' 
    and deleted = false
    and data#>'{@graph, 1, instanceOf, intendedAudience}' @> '[{"@type": "IntendedAudience"}]'
"""

selectBySqlWhere(where) { bib ->
    def instance = bib.graph[1]
    def work = instance.instanceOf
    if (work.intendedAudience.removeAll { asList(it.label).contains('För skolbruk') }) {
        instance['hasNote'] = asList(instance['hasNote'])
        instance['hasNote'].add(['@type': 'Note', 'label': 'För skolbruk'])
        if (work.intendedAudience.isEmpty()) {
            work.remove('intendedAudience')
        }
        bib.scheduleSave()
    }
}
