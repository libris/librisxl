/**
 * Replace local supplementTo with link
*  Based on ../../2025/10/lxl-4710-link-ispartof-once-more.groovy
 * See https://kbse.atlassian.net/browse/LXL-4815
 */

import java.util.concurrent.ConcurrentHashMap

String where = """
    collection = 'bib' and deleted = false and data#>>'{@graph,1,supplementTo}' LIKE '%"controlNumber":%'
"""

skipped = getReportWriter("skipped")
info = getReportWriter("info")
modified_flerband = getReportWriter("modified-flerband")
modified_huvudpost = getReportWriter("modified-huvudpost")

def whelk = getWhelk()
Set processedTargets = ConcurrentHashMap.newKeySet()
Set processedTargetsToSkip = ConcurrentHashMap.newKeySet()

selectBySqlWhere(where) { doc ->
    def sourceThing = doc.graph[1]
    def _logSkip = { msg -> skipped.println("${doc.doc.getURI()}: ${msg}") }
    def _logInfo = { msg -> info.println("Source: ${doc.doc.getURI()} ${msg}") }

    def supplementTos = asList(sourceThing["supplementTo"])

    if (supplementTos.size() != 1) {
        _logSkip("more than one supplementTo")
        return
    }
    def supplementTo = supplementTos[0]

    if (supplementTo["describedBy"].size() != 1) {
        _logSkip("more than one describedBy: ${supplementTo.describedBy}")
        return
    }

    def describedBy = supplementTo["describedBy"][0]
    if (!(describedBy instanceof Map && describedBy.keySet().equals(["@type", "controlNumber"].toSet()))) {
        _logSkip("describedBy contains something other than [@type, controlNumber]: ${describedBy.keySet()}")
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

    String properUri = findMainEntityId(sanitize(describedBy["controlNumber"]))
    if (properUri == null) {
        _logSkip("couldn't find target")
        return
    }
    def targetDoc = whelk.storage.loadDocumentByMainId(properUri)
    def targetRecord = targetDoc.data["@graph"][0]
    def targetThing = targetDoc.data["@graph"][1]

    // Sanity check
    if (doc.doc.getShortId() == targetDoc.getShortId()) {
        _logSkip("Source and target are equal! NOPEing out.")
        return
    }
    if (!targetRecord.technicalNote?.any {
        it instanceof Map &&
        it["@type"] == "TechnicalNote" &&
        "Huvudpost (flerbandsverk)" in [it["label"]].flatten()
    }) {
        _logSkip("correct TechnicalNote not found in target ${targetDoc.getShortId()}")
        return false
    }

    if (supplementTo.containsKey("marc:displayText")) {
        _logSkip("skipping due to marc:displayText:: ${supplementTo['marc:displayText']}")
        // Once *all* supplementTos have been procesed, *then* we remove TechnicalNote
        // from the targets. However, it can (maybe) happen that for a given target there's
        // one supplementTo with marc:displayText and one without. In that case, we'll have to
        // handle the one *with* marc:displayText at some later point, and until that happens
        // we don't want to remove TechnicalNote from the target.
        // So the following is to track what targets we should exclude from TechnicalNote removal.
        processedTargetsToSkip.add(targetDoc.getShortId())
        return
    }

    processedTargets.add(targetDoc.getShortId())

    sourceThing.remove("supplementTo")
    def isPartOf = asList(sourceThing["isPartOf"])
    if (!isPartOf.any { it['@id'] == properUri }) {
        isPartOf << ["@id": properUri]
        sourceThing["isPartOf"] = isPartOf
    }

    modified_flerband.println(doc.doc.getShortId())
    doc.scheduleSave()
}

processedTargets.removeAll(processedTargetsToSkip)

selectByIds(processedTargets) { doc ->
    def record = doc.graph[0]
    def technicalNotes = asList(record.technicalNote)

    technicalNotes.removeIf {
        it instanceof Map &&
        it["@type"] == "TechnicalNote" &&
        "Huvudpost (flerbandsverk)" in [it["label"]].flatten()
    }

    if (technicalNotes.isEmpty()) {
        record.remove("technicalNote")
    } else {
        record.technicalNote = technicalNotes
    }

    modified_huvudpost.println(doc.doc.getShortId())
    doc.scheduleSave()
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
    return null
}

static String sanitize(String value) {
    return value.replaceAll(/(?U)[\s\p{Cntrl}\p{Cf}]/, '')
}