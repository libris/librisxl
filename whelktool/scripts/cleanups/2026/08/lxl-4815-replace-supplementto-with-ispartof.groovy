/**
 * Replace local supplementTo with link
*  Based on ../../2025/10/lxl-4710-link-ispartof-once-more.groovy
 * See https://kbse.atlassian.net/browse/LXL-4815
 */

import java.util.concurrent.ConcurrentHashMap
import whelk.util.Unicode

String where = """
    collection = 'bib' and deleted = false
    and jsonb_path_exists(data, '\$."@graph"[1].supplementTo[*].describedBy[*].controlNumber')
    and not jsonb_path_exists(data, '\$."@graph"[1].supplementTo[*]."marc:displayText" ? (@ == "channel record")')
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

    if (supplementTo["describedBy"]?.size() != 1) {
        _logSkip("more than one describedBy: ${supplementTo.describedBy}")
        return
    }

    def describedBy = supplementTo["describedBy"][0]
    if (!(describedBy instanceof Map && describedBy.keySet().equals(["@type", "controlNumber"].toSet()))) {
        _logSkip("describedBy contains something other than [@type, controlNumber]: ${describedBy.keySet()}")
        return
    }

    String targetControlNumber
    if (describedBy["controlNumber"] instanceof String) {
        targetControlNumber = describedBy["controlNumber"]
    } else if (describedBy["controlNumber"] instanceof List) {
        targetControlNumber = describedBy["controlNumber"].find { it instanceof String && it.startsWith("99") }
    } else {
        _logSkip("controlNumber not a string nor a list: ${describedBy['controlNumber'].getClass()}")
        return
    }
    if (!targetControlNumber) {
        _logSkip("no valid controlNumber found")
        return
    }
    targetControlNumber = sanitize(targetControlNumber)
    if (targetControlNumber.length() < 4) {
        _logSkip("controlNumber suspiciously short: ${describedBy['controlNumber']}")
        return
    }

    List properUris = findMainEntityIds(targetControlNumber)
    String properUri
    if (properUris.size() == 0) {
        if (targetControlNumber.startsWith("99") && targetControlNumber.length() == 10 && !isOtherMainEntityId(targetControlNumber)) {
            _logSkip("could not find target ${targetControlNumber}, looks like LibrisIII; NOT removing (but probably should be removed)")
            /*supplementTos[0].remove("describedBy")
            if (supplementTos[0].size() == 0) {
                _logInfo("supplementTo[0] now empty; removing")
                supplementTos.remove(0)
            }
            if (supplementTos.size() == 0) {
                _logInfo("supplementTo now empty; removing")
                sourceThing.remove("supplementTo")
            } else {
                sourceThing.supplementTo = supplementTos
            }
            doc.scheduleSave()*/
        } else {
            _logSkip("could not find target ${targetControlNumber}")
        }
        return
    } else if (properUris.size() > 1) {
        _logSkip("found more than one target: ${properUris}")
        return
    } else {
        properUri = properUris[0]
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
        return
    }

    if (supplementTo.containsKey("marc:displayText")) {
        _logSkip("skipping due to marc:displayText: ${supplementTo['marc:displayText']}")
        // Once *all* supplementTos have been processed, *then* we remove TechnicalNote
        // from the targets. However, it can (maybe) happen that for a given target there's
        // one supplementTo with marc:displayText and one without. In that case, we'll have to
        // handle the one *with* marc:displayText at some later point, and until that happens
        // we don't want to remove TechnicalNote from the target.
        // So the following is to track what targets we should exclude from TechnicalNote removal.
        processedTargetsToSkip.add(targetDoc.getShortId())
        return
    }

    // Added to do a quick check; deemed unnecessary; might be useful later for less simple cases
/*
    def supplementToMainTitle = asList(getAtPath(supplementTo, ['hasTitle', 0, 'mainTitle'], [])).join(" ")
    if (supplementToMainTitle) {
        def targetMainTitle = asList(getAtPath(targetThing, ['hasTitle', 0, 'mainTitle'], [])).join(" ")
        def (isSameTitle, titleResult) = isSeeminglySameTitle(supplementToMainTitle, targetMainTitle, properUri)
        if (!isSameTitle) {
            _logSkip("title mismatch: source: ${titleResult.source}, target: ${titleResult.target}, words not in target: ${titleResult.notInTarget}. Target ${properUri}")
            _logSkip("title: ${supplementToMainTitle}\t\t${targetMainTitle}")
            return
        }
    } else {
        _logInfo("no title in supplementTo")
    }
*/
    processedTargets.add(targetDoc.getShortId())

    sourceThing.remove("supplementTo")
    def isPartOf = asList(sourceThing["isPartOf"])
    if (isPartOf.any { it['@id'] == properUri }) {
        _logInfo("target already linked in isPartOf, just removing supplementTo")
    } else {
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

List<String> findMainEntityIds(String ctrlNumber) {
    //String mainId = null
    //try {
    //    mainId = findCanonicalId("${baseUri.resolve(ctrlNumber)}#it")
    //} catch (IllegalArgumentException e) {
    //}
    //if (mainId) {
    //    return [mainId]
    //}
    //def legacyId = "http://libris.kb.se/resource/bib/${ctrlNumber}"
    //mainId = findCanonicalId(legacyId)
    //if (mainId) {
    //    return [mainId]
    //}

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
    if (byLibris3Ids.size() > 0) {
        return byLibris3Ids
    }
    return []
}

boolean isOtherMainEntityId(String ctrlNumber) {
    String mainId = null
    try {
        mainId = findCanonicalId("${baseUri.resolve(ctrlNumber)}#it")
    } catch (IllegalArgumentException e) {
    }
    if (mainId) {
        return true
    }
    def legacyId = "http://libris.kb.se/resource/bib/${ctrlNumber}"
    mainId = findCanonicalId(legacyId)
    if (mainId) {
        return true
    }
    return false
}

static String sanitize(String value) {
    return value.replaceAll(/(?U)[\s\p{Cntrl}\p{Cf}]/, '')
}

static List isSeeminglySameTitle(String sourceTitle, String targetTitle, String targetProperUri) {
    boolean isMatch = false

    Set sourceTitleWords = extractWords(sourceTitle) as Set
    Set targetTitleWords = extractWords(targetTitle) as Set
    List wordsToRemove = [
        "ljudupptagning",
        "red",
        "av",
        "redaktor",
        "och",
        "by",
        "de",
        "den",
        "dem",
        "ett",
        "edited",
        "redaktion",
        "1",
        "2",
        "utgiven",
        "i",
        "for",
        "redigerad",
        "vol",
        "huvudredaktor",
        "redaktionskommitte",
        "suecana",
        "elektronisk",
        "sammanstallare",
        "resurs",
        "tidskrift",
        "medlemstidning",
        "utgivare",
        "foreningen",
        "editor",
        "forfattare",
        "sammanstallning",
        "sammanstalld",
        "urval",
        "huvudred",
        "redaktorer",
        "sverges",
        "idellt",
        "titelflt",
        "saknas",
        "okober",
        "the",
        "of",
        "in",
        "a",
        "an",
        "and",
        "by",
        "der",
        "die",
        "das",
        "roman",
        "report",
        "rapport",
        "en",
        "le",
        "la",
        "les",
        "un",
        "une",
        "et",
        "und",
        "e",
        "y",
    ]
    sourceTitleWords.removeAll(wordsToRemove)
    targetTitleWords.removeAll(wordsToRemove)

    if (targetTitleWords.containsAll(sourceTitleWords) || sourceTitleWords.containsAll(targetTitleWords)) {
        isMatch = true
    }

    String info

    if (!isMatch) {
        def wordDiff = sourceTitleWords - targetTitleWords
        def threshold = Math.max(1, (int) Math.ceil(sourceTitleWords.size() * 0.3))
        def sourceTitleJoined = sourceTitleWords.join("").take(100)
        def targetTitleJoined = targetTitleWords.join("").take(100)

        if (wordDiff.size() < threshold) {
            isMatch = true
        } else if (Unicode.damerauLevenshteinDistance(sourceTitleJoined, targetTitleJoined) < 3 && sourceTitleJoined.length() > 9 && targetTitleJoined.length() > 9) {
            isMatch = true
        } else {
            info = "${sourceTitle}\nTarget: ${targetProperUri} ${targetTitle}\n${wordDiff.size()} source word(s) not anywhere in whole target record: ${wordDiff}\n\n"
        }
    }

    return [
        isMatch,
        [
            source: sourceTitleWords,
            target: targetTitleWords,
            //difference: (targetTitleWords + sourceTitleWords) - sourceTitleWords.intersect(targetTitleWords)
            notInTarget: sourceTitleWords - targetTitleWords,
            info: info,
        ]
    ]
}

static Set extractWords(String title) {
    return Unicode.removeAllDiacritics(title).replaceAll(/\//, " ").replaceAll("-", " ").replaceAll(/[^a-zA-Z0-9 ]/, "").toLowerCase().split('\\s+')
}
