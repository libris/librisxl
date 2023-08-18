/**
 * Move summary supplied by some providers from work to instance
 *
 * See LXL-3303 for more information
 */

providers = [
        '[Barnbokskatalogen]',
        '[Elib]',
        '[Publit]',
        'Provided by publisher'
]

def where = """
  collection = 'bib' 
  AND data#>>'{@graph, 1, instanceOf, @type}' = 'Text'
  AND data#>>'{@graph, 1, instanceOf, summary}' IS NOT NULL
  AND deleted = false
  """

Set shape = ['@type', 'label'] as Set

selectBySqlWhere(where) { bib ->
    List summaries = bib.graph[1]['instanceOf']['summary']
    def (toInstance, toWork) = summaries.split { Map s ->
        s.keySet() == shape
                && providers.any { p -> asList(s.label).any { l -> l.contains(p) } }
    }

    if (toInstance) {
        if (toWork) {
            bib.graph[1]['instanceOf']['summary'] = toWork
        } else {
            bib.graph[1]['instanceOf'].remove('summary')
        }

        bib.graph[1]['summary'] = (bib.graph[1]['summary'] ?: []) + toInstance
        bib.scheduleSave()
    }
}

private List asList(Object o) {
    if (o == null)
        return []
    if (o instanceof List)
        return o
    return [o]
}
