/**
 * Adapted from ../06/lxl-3580-add-sigel-dst-to-digi.groovy
 * 
 * Adds sigel DST to @graph,0,bibliography in records having sigel DIGI and fulfilling
 * various criteria.
 * 
 * See LXL-3696 LXL-3580 for more info.
 */

String where = """
  collection = 'bib' 
  AND deleted = false
  AND data#>>'{@graph,1,@type}' = 'Electronic'
  AND data#>>'{@graph,0,bibliography}' LIKE '%https://libris.kb.se/library/DIGI%'
  AND (
    data#>>'{@graph,1,publication}' LIKE '%https://id.kb.se/country/sw%' 
    OR data#>>'{@graph,1,publication}' LIKE '%https://id.kb.se/country/fi%'
  )
  AND (
    data#>>'{@graph,1,associatedMedia}' LIKE '%Fritt tillg%' 
    OR data#>>'{@graph,1,usageAndAccessPolicy}' LIKE '%gratis%'
  )
"""

selectBySqlWhere(where) { data ->
    Map record = data.graph[0]
    Map thing = data.graph[1]

    if (record.bibliography?.any { it.'@id' == 'https://libris.kb.se/library/DST' }) {
        return // Nothing to do here!
    }

    if (isDst(record, thing)) {
        record.bibliography << ['@id': 'https://libris.kb.se/library/DST']
        data.scheduleSave()
    }
}

boolean isDst(Map record, Map thing) {
    thing.'@type' == 'Electronic' && 
            record.bibliography?.any { it['@id'] == 'https://libris.kb.se/library/DIGI' } && 
            thing.publication?.any { publishedIn(it, 'sw') || (publishedIn(it, 'fi') && pre1810(it)) } &&
            isFritt(thing)
}

boolean pre1810(publication) {
    parseYear(publication.year) <= 1809 || parseYear(publication.date) <= 1809
}

int parseYear(String date) {
    (date && date ==~ /\d\d\d\d.*/) 
            ? Integer.parseInt(date.substring(0,4)) 
            : Integer.MAX_VALUE
}

boolean publishedIn(Map publication, countryCode) {
    publication.country && publication.country['@id'] == "https://id.kb.se/country/$countryCode"
}

boolean isFritt(thing) {
    asList(thing.associatedMedia).any { hasPublicAssociatedMedia(it) } || 
            (asList(thing.associatedMedia).any{ hasAssociatedMedia(it) } && isGratis(thing))
}

boolean isGratis(Map thing) {
    thing.usageAndAccessPolicy && asList(thing.usageAndAccessPolicy).any {
        asList(it.label).any{ String label -> label.equalsIgnoreCase('gratis')}
    }
}

boolean hasAssociatedMedia(media) {
    media instanceof Map && media.uri
}

private boolean hasPublicAssociatedMedia(media) {
    if (!hasAssociatedMedia(media)) {
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
