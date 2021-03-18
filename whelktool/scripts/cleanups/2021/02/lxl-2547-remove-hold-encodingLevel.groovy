/**
 * Removes encodingLevel from hold records.
 *
 * See LXL-2547 for more info.
 */

String where = """
  collection = 'hold' AND
  deleted = false AND
  data#>>'{@graph,0,encodingLevel}' IS NOT NULL
  """

selectBySqlWhere(where) { data ->
  if(data.graph[0].remove('encodingLevel'))
    data.scheduleSave()
}
