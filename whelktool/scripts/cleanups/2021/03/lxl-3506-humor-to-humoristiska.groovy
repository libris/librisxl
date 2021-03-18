/**
 * Changes genreForm 'Humor' (c9ps0zlw18d8n11) to 'Humoristiska skildringar'
 * (cq2d5xpl9pl9zrst) for instances having one of the MARC fiction types.
 *
 * See LXL-3506 for more info.
 */

import whelk.util.DocumentUtil

List MARC_FICTION_TYPES = [
  "https://id.kb.se/marc/FictionNotFurtherSpecified",
  "https://id.kb.se/marc/Drama",
  "https://id.kb.se/marc/Essay",
  "https://id.kb.se/marc/Novel",
  "https://id.kb.se/marc/HumorSatiresEtc",
  "https://id.kb.se/marc/ShortStory",
  "https://id.kb.se/marc/MixedForms",
  "https://id.kb.se/marc/Poetry",
]

String OLD_GF = "https://id.kb.se/term/saogf/Humor"
String NEW_GF = "https://id.kb.se/term/saogf/Humoristiska%20skildringar"

String where = """
  collection = 'bib' AND
  deleted = false AND
  id IN (SELECT id FROM lddb__dependencies WHERE dependsonid = 'c9ps0zlw18d8n11')
  """

selectBySqlWhere(where) { data ->
  def thing = data.graph[1]
  boolean modified = false

  if (! thing.instanceOf?.genreForm?.any { gf -> MARC_FICTION_TYPES.contains(gf['@id'])} ) {
    return
  }

  List genreForm = thing.instanceOf.genreForm
  DocumentUtil.traverse(genreForm) { value, path ->
      if (path && path.last() == '@id' && value == OLD_GF) {
        modified = true
        if (['@id': NEW_GF] in genreForm) {
          return new DocumentUtil.Remove()
        } else {
          return new DocumentUtil.Replace(NEW_GF)
        }
      }
  }

  if (modified)
    data.scheduleSave()
}
