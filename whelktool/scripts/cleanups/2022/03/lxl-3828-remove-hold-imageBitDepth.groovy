/**
 * Removes imageBitDepth from hold records. (All have only '---' as values).
 *
 * See LXL-3828 for more info.
 */

String where = """
  collection = 'hold' AND
  deleted = false AND
  data#>>'{@graph,1,imageBitDepth}' IS NOT NULL
  """

selectBySqlWhere(where) { data ->
  if(data.graph[1].remove('imageBitDepth'))
    data.scheduleSave()
}
