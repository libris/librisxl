/**
 * Removes sameAs from closeMatch in auth records.
 *
 * See LXL-3512 for more info.
 */

String where = """
  collection = 'auth' AND
  deleted = false AND
  data#>>'{@graph,1,closeMatch}' LIKE '%"sameAs":%'
  """

selectBySqlWhere(where) { data ->
  boolean modified = false

  data.graph[1].closeMatch?.each {
    if (it.sameAs) {
      it.remove('sameAs')
      modified = true
    }
  }

  if (modified)
    data.scheduleSave()
}
