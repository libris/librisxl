import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

/**
 * Adapted from ../../2021/10/lxl-3696-add-sigel-dst-to-digi-2.groovy
 *
 * Adds sigel DST to @graph,0,bibliography in records having sigel DIGI and fulfilling
 * various criteria. Also creates DST holdings with sigel DST for all bib records included in the DST bibliography.
 *
 *
 * (Copied from ../../2022/02/lxl-3785-supplementTo-isIssueOf.groovy)
 * SYSTEM PARAMETERS:
 * -Dlast-run-file=</path/to/file>
 *     If given, a timestamp of this run is saved in this file, and used to only select new records on the next run.
 *
 *
 * See LXL-3696, LXL-3580 and LXL-3903 for more info.
 */

String now = ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)

String lastRunOpt = System.getProperty('last-run-file', '')
def lastRunTimestamp = new File(lastRunOpt).with { if (it.exists()) it.readLines()[0] }
println("Using lastRunTimestamp: ${lastRunTimestamp ?: '<NO>'}")

String where = """
  collection = 'bib' 
  AND deleted = false
  AND data#>>'{@graph,1,@type}' = 'Electronic'
  AND data#>'{@graph,0,bibliography}' @> '[{"@id": "https://libris.kb.se/library/DIGI"}]'
  AND (
    data#>>'{@graph,1,publication}' LIKE '%https://id.kb.se/country/sw%' 
    OR data#>>'{@graph,1,publication}' LIKE '%https://id.kb.se/country/fi%'
  )
  AND (
    data#>>'{@graph,1,associatedMedia}' LIKE '%Fritt tillg%' 
    OR data#>>'{@graph,1,usageAndAccessPolicy}' LIKE '%gratis%'
  )
  ${lastRunTimestamp ? "AND created >= '$lastRunTimestamp'" : ''}
"""

// Add DST to bibliography where missing.
selectBySqlWhere(where) { data ->
    Map record = data.graph[0]
    Map thing = data.graph[1]

    if (record.bibliography?.any { it.'@id' == 'https://libris.kb.se/library/DST' }) {
        return // Nothing to do here!
    }

    if (isDst(record, thing)) {
        record.bibliography << ['@id': 'https://libris.kb.se/library/DST']
        data.scheduleSave(loud: true)
    }
}

String whereIsDstBib = """
  collection = 'bib' 
  AND deleted = false
  AND data#>'{@graph,0,bibliography}' @> '[{"@id": "https://libris.kb.se/library/DST"}]'
  ${lastRunTimestamp ? "AND created >= '$lastRunTimestamp'" : ''}
"""

Set dstBib = Collections.synchronizedSet([] as Set)

selectBySqlWhere(whereIsDstBib) {
    dstBib.add(it.graph[1]['@id'])
}

String whereIsDstHold = """
  collection = 'hold' 
  AND deleted = false
  AND data#>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/DST'
  ${lastRunTimestamp ? "AND created >= '$lastRunTimestamp'" : ''}
"""

Set dstBibWithDstHold = Collections.synchronizedSet([] as Set)

selectBySqlWhere(whereIsDstHold) {
    dstBibWithDstHold.add(it.graph[1]['itemOf']['@id'])
}

// Create new DST hold record for each bib record that didn't have one before
Set dstBibLackingDstHold = dstBib - dstBibWithDstHold
selectFromIterable(dstBibLackingDstHold.collect { createHold(it) }) { hold ->
    hold.scheduleSave(loud: true)
}

if (lastRunOpt) {
    new File(lastRunOpt).withWriter { writer ->
        writer.write(now)
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
    publication.country && asList(publication.country).find()['@id'] == "https://id.kb.se/country/$countryCode"
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

def createHold(String bibMainEntityId) {
    def data =
            [ "@graph": [
                    [
                            "@id": "TEMPID",
                            "@type": "Record",
                            "mainEntity" : ["@id": "TEMPID#it"]
                    ],
                    [
                            "@id": "TEMPID#it",
                            "@type": "Item",
                            "heldBy": ["@id": "https://libris.kb.se/library/DST"],
                            "itemOf": ["@id": bibMainEntityId],
                            "hasComponent": [
                                    [
                                            "@type": "Item",
                                            "heldBy": ["@id": "https://libris.kb.se/library/DST"]
                                    ]
                            ]
                    ]
            ]]

    return create(data)
}

