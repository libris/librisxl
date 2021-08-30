/**
 * Adds sigel DST to @graph,0,bibliography in records having sigel DIGI and fulfilling
 * various criteria.
 * 
 * See LXL-3580 for more info.
 */

String where = """
  collection = 'bib' AND
  deleted = false AND
  data#>>'{@graph,0,bibliography}' LIKE '%https://libris.kb.se/library/DIGI%' AND
  data#>>'{@graph,1,publication}' LIKE '%https://id.kb.se/country/sw%' AND
  data#>>'{@graph,1,@type}' = 'Electronic' AND
  data#>>'{@graph,1,associatedMedia}' LIKE '%Fritt tillg%'
  """

selectBySqlWhere(where) { data ->
  boolean modified = false
  def record = data.graph[0]
  def thing = data.graph[1]

  if (record.bibliography?.any { it.'@id' == 'https://libris.kb.se/library/DST' }) {
    return // Nothing to do here!
  }

  if (
    record.bibliography?.any { it.'@id' == 'https://libris.kb.se/library/DIGI' } &&
    thing.publication?.any { it.country && it.country.'@id' == 'https://id.kb.se/country/sw' } &&
    thing.'@type' == 'Electronic' &&
    asList(thing.associatedMedia).any { hasPublicAssociatedMedia(it) }
    ) {
    record.bibliography << ['@id': 'https://libris.kb.se/library/DST']
    modified = true
  }

  if (modified) {
    data.scheduleSave()
  }
}

private boolean hasPublicAssociatedMedia(media) {
  if (!media instanceof Map || !media.uri) {
    return false
  }

  if (media.'marc:publicNote' instanceof String && media.'marc:publicNote'.startsWith('Fritt tillg')) {
    return true
  }

  if (media.'marc:publicNote' instanceof List && media.'marc:publicNote'.any { it.startsWith('Fritt tillg') }) {
    return true
  }

  return false
}

private List asList(Object o) {
    (o instanceof List) ? (List) o : (o ? [o] : [])
}
