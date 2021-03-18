/**
 * Adds default encodingLevel to bib records with missing encodingLevel
 *
 * See LXL-2479 for more info.
 */

String where = """
  collection = 'bib' AND
  deleted = false AND
  data#>>'{@graph,0,encodingLevel}' IS NULL
  """

selectBySqlWhere(where) { data ->
  if (!data.graph[0].encodingLevel) {
    data.graph[0].encodingLevel = "marc:AbbreviatedLevel"
    data.scheduleSave()
  }
}
