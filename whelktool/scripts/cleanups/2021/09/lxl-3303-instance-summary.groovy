/**
 * Move summary supplied by some providers from work to instance
 * 
 * See LXL-3303 for more information
 */

providers = [
        '[Barnbokskatalogen]',
        '[Elib]',
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
    def (toInstance, toWork) = summaries.split { 
        Map s -> s.keySet() == shape && providers.any{s.label.contains(it) } 
    }
    
    if (toInstance) {
        bib.graph[1]['instanceOf']['summary'] = toWork
        bib.graph[1]['summary'] = bib.graph[1]['summary'] ?: [] + toInstance
        bib.scheduleSave()
    }
}
