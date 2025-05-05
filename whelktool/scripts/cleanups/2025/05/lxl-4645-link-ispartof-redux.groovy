/**
 * Replace local isPartOf with link
 * Some code borrowed from ../../2020/11/lxl-3379-link-isPartOf.groovy
 * See https://kbse.atlassian.net/browse/LXL-4645
 */

String where = """
    collection = 'bib' and deleted = false and data#>>'{@graph,1,isPartOf}' LIKE '%"controlNumber":%'
"""

skipped = getReportWriter("skipped")

selectBySqlWhere(where) { doc ->
    def source_thing = doc.graph[1]
    def _logSkip = { msg -> skipped.println("${doc.doc.getURI()}: ${msg}") }

    List isPartOfs = asList(source_thing["isPartOf"])
    if (isPartOfs.size() != 1) {
        _logSkip("more than one isPartOf")
        return
    }

    def isPartOf = isPartOfs[0]

    if (!isPartOf.keySet().equals(["@type", "hasTitle", "describedBy", "identifiedBy"].toSet())) {
        _logSkip("isPartOf contains something other than [@type,hasTitle,describedBy,identifiedBy]: ${isPartOf.keySet()}")
        return
    }

    if (!(whelk.jsonld.isSubClassOf(isPartOf["@type"], "Instance"))) {
        _logSkip("isPartOf.@type not Instance: ${isPartOf['@type']}")
        return
    }

    if (isPartOf["describedBy"].size() != 1) {
        _logSkip("more than one describedBy: ${isPartOf.describedBy}")
        return
    }

    if (isPartOf["identifiedBy"].size() != 1) {
        _logSkip("more than one identifiedBy: ${isPartOf.identifiedBy}")
        return
    }

    if (isPartOf["hasTitle"].size() != 1) {
        _logSkip("more than one hasTitle: ${isPartOf.hasTitle}")
        return
    }

    if (!isPartOf["hasTitle"][0].keySet().equals(["@type", "mainTitle"].toSet())) {
        _logSkip("hasTitle[0] has something other than @type and mainTitle: ${isPartOf["hasTitle"][0].keySet()}")
        return
    }

    if (!isPartOf["identifiedBy"][0].keySet().equals(["@type", "value"].toSet())) {
        _logSkip("identifiedBy has something other than [@type,value]: ${isPartOf["identifiedBy"][0].keySet()}")
        return
    }

    if (!(isPartOf["identifiedBy"][0]["@type"] in ["ISSN", "ISBN"])) {
        _logSkip("identifiedBy.@type is neither ISSN nor ISBN; found ${isPartOf['identifiedBy'][0]['@type']}")
        return
    }
    String sourceIdentifiedByType = isPartOf["identifiedBy"][0]["@type"]
    String sourceIdentifiedByValue = isPartOf["identifiedBy"][0]["value"]
    // Some docs have the ISSN value prefixed with "ISSN "...
    if (sourceIdentifiedByValue.startsWith("ISSN ")) {
        sourceIdentifiedByValue = sourceIdentifiedByValue.substring("ISSN ".size())
    }

    def describedBy = isPartOf["describedBy"][0]
    if (!(describedBy instanceof Map && describedBy.keySet().equals(["@type", "controlNumber"].toSet()))) {
        _logSkip("describedBy contains something other than [@type,controlNumber]: ${describedBy.keySet()}")
        return
    }
    if (!(describedBy["controlNumber"] instanceof String)) {
        _logSkip("controlNumber not a string: ${describedBy['controlNumber']}")
        return  
    }

    if (describedBy["controlNumber"].length() < 4) {
        _logSkip("controlNumber suspiciously short: ${describedBy['controlNumber']})")
        return
    }

    if (!(isPartOf["hasTitle"][0]["mainTitle"] instanceof String)) {
        _logSkip("hasTitle.mainTitle not a string")
        return
    }
    def sourceTitle = isPartOf["hasTitle"][0]["mainTitle"].trim()
    if (sourceTitle == "") {
        _logSkip("Skipping because of empty sourceTitle")
        return 
    }
    String properUri = findMainEntityId(sanitize(describedBy["controlNumber"]))
    if (properUri != null) {
        def targetDoc = whelk.storage.loadDocumentByMainId(properUri)
        def targetThing = targetDoc.data["@graph"][1]

        if (!(whelk.jsonld.isSubClassOf(targetThing["@type"], "Instance"))) {
            _logSkip("@type not Instance (or subclass thereof) in target ${properUri}: ${targetThing['@type']}")
            return
        }

        List targetTitles = []
        targetThing.hasTitle.each {
            if (it["@type"] == "Title" && it.mainTitle) {
                targetTitles << it["mainTitle"].trim()
            }
            if (it["@type"] == "KeyTitle" && it.mainTitle && it.qualifier) {
                targetTitles << "${it.mainTitle} ${it['qualifier'][0]}".trim()
            } else if (it["@type"] == "KeyTitle" && it.mainTitle) {
                targetTitles << it.mainTitle.trim()
            }
        }
        if (!(targetTitles.any { it.equalsIgnoreCase(sourceTitle) })) {
            _logSkip("title mismatch: '${sourceTitle}' [source] does not match mainTitle or mainTitle+qualifier ${targetTitles} in target ${properUri}")
            return
        }

        List targetIdentifiers = getAtPath(targetThing, ['identifiedBy', '*'], [])

        List filteredIdentifiers = targetIdentifiers.findAll { it.containsKey("@type") && it.containsKey("value") && it["@type"] == sourceIdentifiedByType }
        if (filteredIdentifiers.size() == 0) {
            _logSkip("no identifier with type ${sourceIdentifiedByType} in target ${properUri}, probably LibrisIIINumber")
            return
        }
        if (filteredIdentifiers.size() > 1) {
            _logSkip("found more than one identifier with type ${sourceIdentifiedByType} in target ${properUri}")
            return
        }
        // ignoreCase necessary: 0021-406x vs. 0021-406X
        if (!filteredIdentifiers[0]["value"].equalsIgnoreCase(sourceIdentifiedByValue)) {
            _logSkip("identifiedBy.value mismatch: ${sourceIdentifiedByValue} in source, ${filteredIdentifiers[0]['value']} in target ${properUri}")
            return
        }

        source_thing["isPartOf"][0].clear()
        source_thing["isPartOf"][0]["@id"] = properUri

        doc.scheduleSave()
    }
}

String findMainEntityId(String ctrlNumber) {
    String mainId = null
    try {
        mainId = findCanonicalId("${baseUri.resolve(ctrlNumber)}#it")
    } catch (IllegalArgumentException e) {
    }
    if (mainId) {
        return mainId
    }
    def legacyId = "http://libris.kb.se/resource/bib/${ctrlNumber}"
    mainId = findCanonicalId(legacyId)
    if (mainId) {
        return mainId
    }

// Skipping the following for now to focus on the really simple stuff.
// There are targets with only LibrisIIINumber in their identifiedBy.
// These we'll probably want to enrich with data from the source but that's
// a later problem.
/*
    def byLibris3Ids = []
    // IMPORTANT: This REQUIRES an index on '@graph[0]identifiedBy*.value'.
    // If that is removed, this slows to a GLACIAL crawl!
    ctrlNumber = ctrlNumber.replaceAll(/['"\\]/, '')
    selectBySqlWhere("""
    data #> '{@graph,0,identifiedBy}' @> '[{"@type": "LibrisIIINumber", "value": "${ctrlNumber}"}]'::jsonb and collection = 'bib'
    """, silent: true) {
        List mainEntityIDs = it.doc.getThingIdentifiers()
        if (mainEntityIDs.size() > 0)
            byLibris3Ids << mainEntityIDs[0]
    }
    if (byLibris3Ids.size() == 1) {
        return byLibris3Ids[0]
    }
*/
    return null
}

static String sanitize(String value) {
    return value.replaceAll(/\9/, '')
}
