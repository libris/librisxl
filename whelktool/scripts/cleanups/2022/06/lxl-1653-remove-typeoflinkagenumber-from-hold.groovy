/*
 * This removes marc:typeOfLinkageNumber and solves both incorrect usage of indicators in 014 and faulty modeling leaving it outside the LibrisIIINumber entity.
 *
 * See LXL-1653 for more info.
 *
 */

String where = """
  collection = 'hold' AND
  deleted = false AND
  data#>>'{@graph,0,marc:typeOfLinkageNumber}' IS NOT NULL
  """

selectBySqlWhere(where) { data ->
  if(data.graph[0].remove('marc:typeOfLinkageNumber'))
    data.scheduleSave()
}