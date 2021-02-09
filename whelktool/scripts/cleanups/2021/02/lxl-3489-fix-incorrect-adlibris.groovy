/**
 * Fixes Adlibris records that for some time got incorrect language and/or genreForm
 *
 * See LXL-3489 for more info.
 */

String where = """
  collection = 'bib' AND
  deleted = false AND
  created >= '2020-03-20' AND
  created  < '2021-01-20' AND
  data#>>'{@graph,0,identifiedBy}' like '%Adlibris%' AND
  (
    data#>>'{@graph,1,instanceOf,genreForm}' like '%"code"%' OR
    data#>>'{@graph,1,instanceOf,language}' like '%"code"%'
  )
  """

selectBySqlWhere(where) { data ->
  def instance = data.graph[1]
  boolean modified = false

  ["genreForm", "language"].each { prop ->
    modified |= instance.instanceOf[prop]?.removeAll { it instanceof Map && it.size() == 1 && it.code }
    if (instance.instanceOf[prop]?.size() == 0) {
      instance.instanceOf.remove(prop)
    }
  }

  if (modified) {
    data.scheduleSave()
  }
}
