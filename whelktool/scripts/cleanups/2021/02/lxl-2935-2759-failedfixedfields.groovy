/**
 * Removes 17 (encodingLevel) from _marcFailedFixedFields for bib and hold records.
 *
 * See LXL-2935 and LXL-2759 for more info.
 */

PrintWriter modifiedBibs = getReportWriter("modified-bibs")
PrintWriter modifiedHolds = getReportWriter("modified-holds")

String where = """
  (collection = 'bib' or collection = 'hold') AND
  deleted = false AND
  data#>>'{@graph,0,_marcFailedFixedFields}' LIKE '%"000"%' AND
  data#>>'{@graph,0,_marcFailedFixedFields}' LIKE '%"17"%'
  """

selectBySqlWhere(where) { data ->
  boolean modified = false
  def marcFailedFixedFields = data.graph[0]['_marcFailedFixedFields']

  if (marcFailedFixedFields.containsKey('000') && marcFailedFixedFields['000'].containsKey('17')) {
    marcFailedFixedFields['000'].remove('17')
    modified = true

    if (!marcFailedFixedFields['000']) {
      marcFailedFixedFields.remove('000')
    }
  }

  if (!marcFailedFixedFields) {
    data.graph[0].remove('_marcFailedFixedFields')
  }

  if (modified) {
    if (data.graph[1]["@type"] == "Item") {
      modifiedHolds.println("${data.doc.getURI()}")
    } else {
      modifiedBibs.println("${data.doc.getURI()}")
    }

    data.scheduleSave()
  }
}
