/*
 * LXL-4775 Remove all classification from auth Person records.
 *
 * Context (2026-03-25):
 * Analysis showed Person.classification was effectively only ClassificationLcc
 * (29 occurrences). We still remove the whole classification property to keep
 * the cleanup simple but probably not safe for reuse on other entities.
 */

String where = """
    collection = 'auth'
    AND deleted = false
    AND data#>>'{@graph,1,@type}' = 'Person'
    AND data#>'{@graph,1,classification}' IS NOT NULL
"""

selectBySqlWhere(where) { data ->
  if(data.graph[1].remove('classification'))
    data.scheduleSave()
}
