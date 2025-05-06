/**
 * Replace local isPartOf with link
 * Some code borrowed from ../../2020/11/lxl-3379-link-isPartOf.groovy
 * See https://kbse.atlassian.net/browse/LXL-4645
 */

String where = """
    collection = 'bib' and deleted = false and data#>>'{@graph,1,isPartOf}' LIKE '%"controlNumber":%'
"""

skipped = getReportWriter("skipped")
def whelk = getWhelk()

selectBySqlWhere(where) { doc ->
    def source_thing = doc.graph[1]
    def _logSkip = { msg -> skipped.println("${doc.doc.getURI()}: ${msg}") }

    List isPartOfs = asList(source_thing["isPartOf"])
    if (isPartOfs.size() != 1) {
        _logSkip("more than one isPartOf")
        return
    }
    def isPartOf = isPartOfs[0]

    def validSets = [
        ["@type", "describedBy"] as Set,
        ["@type", "describedBy", "identifiedBy"] as Set,
        ["@type", "describedBy", "hasTitle"] as Set,
        ["@type", "describedBy", "hasTitle", "identifiedBy"] as Set,
    ]

    if (!(validSets.any { it.equals(isPartOf.keySet()) })) {
        _logSkip("too much stuff in isPartOf: ${isPartOf.keySet()}")
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

    def describedBy = isPartOf["describedBy"][0]
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

    def sourceTitle
    if (isPartOf.containsKey("hasTitle")) {
        if (isPartOf["hasTitle"].size() != 1) {
            _logSkip("more than one hasTitle: ${isPartOf.hasTitle}")
            return
        }

        if (!isPartOf["hasTitle"][0].keySet().equals(["@type", "mainTitle"].toSet())) {
            _logSkip("hasTitle[0] has something other than [@type, mainTitle]: ${isPartOf["hasTitle"][0].keySet()}")
            return
        }

        if (!(isPartOf["hasTitle"][0]["mainTitle"] instanceof String)) {
            _logSkip("hasTitle.mainTitle not a string")
            return
        }

        sourceTitle = isPartOf["hasTitle"][0]["mainTitle"].trim()
        if (sourceTitle == "") {
            _logSkip("empty mainTitle")
            return
        }
    }

    String sourceIdentifiedByType
    String sourceIdentifiedByValue
    if (isPartOf.containsKey("identifiedBy")) {
        if (isPartOf["identifiedBy"].size() != 1) {
            _logSkip("more than one identifiedBy: ${isPartOf.identifiedBy}")
            return
        }

        if (!isPartOf["identifiedBy"][0].keySet().equals(["@type", "value"].toSet())) {
            _logSkip("identifiedBy has something other than [@type, value]: ${isPartOf["identifiedBy"][0].keySet()}")
            return
        }

        if (!(isPartOf["identifiedBy"][0]["@type"] in ["ISSN", "ISBN"])) {
            _logSkip("identifiedBy.@type is neither ISSN nor ISBN; found ${isPartOf['identifiedBy'][0]['@type']}")
            return
        }
        sourceIdentifiedByType = isPartOf["identifiedBy"][0]["@type"]
        sourceIdentifiedByValue = isPartOf["identifiedBy"][0]["value"]
        // Some docs have the ISSN value prefixed with "ISSN "...
        if (sourceIdentifiedByValue.startsWith("ISSN ")) {
            sourceIdentifiedByValue = sourceIdentifiedByValue.substring("ISSN ".size())
        }
        if (sourceIdentifiedByValue.startsWith("ISBN ")) {
            sourceIdentifiedByValue = sourceIdentifiedByValue.substring("ISBN ".size())
        }
        if (sourceIdentifiedByType == "ISBN") {
            sourceIdentifiedByValue = sourceIdentifiedByValue.replace("-", "")
        }
    }

    String properUri = findMainEntityId(sanitize(describedBy["controlNumber"]))
    if (properUri == null) {
        _logSkip("couldn't find target")
        return
    }
    def targetDoc = whelk.storage.loadDocumentByMainId(properUri)
    def targetThing = targetDoc.data["@graph"][1]

    // Sanity check
    if (doc.doc.getShortId() == targetDoc.getShortId()) {
        _logSkip("Source and target are equal! NOPEing out.")
        return
    }

    if (!(whelk.jsonld.isSubClassOf(targetThing["@type"], "Instance"))) {
        _logSkip("@type not Instance (or subclass thereof) in target ${properUri}: ${targetThing['@type']}")
        return
    }

    if (isPartOf.containsKey("hasTitle")) {
        def (isSameTitle, titleResult) = isSeeminglySameTitle(sourceTitle, targetThing.hasTitle)
        if (!isSameTitle) {
            _logSkip("title mismatch: source: ${titleResult.source}, target: ${titleResult.target}, words not in target: ${titleResult.notInTarget}. Target ${properUri}")
            return
        }
    }

    if (isPartOf.containsKey("identifiedBy")) {
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
    }

    source_thing["isPartOf"][0].clear()
    source_thing["isPartOf"][0]["@id"] = properUri

    doc.scheduleSave()
}

List isSeeminglySameTitle(String sourceTitle, List targetHasTitle) {
    boolean isMatch = false
    List targetTitles = []
    targetHasTitle.each {
        if (it["@type"] == "Title") {
            if (it.mainTitle) {
                targetTitles << it.mainTitle
            }
            if (it.subtitle) {
                targetTitles << it.subtitle
            }
        }

        if (it["@type"] == "KeyTitle") {
            if (it.mainTitle) {
                targetTitles << it.mainTitle
            }
            if (it.qualifier) {
                targetTitles << it.qualifier[0]
            }
        }
    }

    Set sourceTitleWords = extractWords(sourceTitle) as Set
    Set targetTitleWords = new HashSet()
    targetTitles.each { targetTitleWords.addAll(extractWords(it)) }

    if (targetTitleWords.containsAll(sourceTitleWords)) {
        isMatch = true
    }

    // TODO add looser matching?

    return [
        isMatch,
        [
            source: sourceTitleWords,
            target: targetTitleWords,
            //difference: (targetTitleWords + sourceTitleWords) - sourceTitleWords.intersect(targetTitleWords)
            notInTarget: sourceTitleWords - targetTitleWords,
        ]
    ]
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
    return value.replaceAll(/\9/, '')
}

static Set extractWords(String title) {
    return title.replaceAll(/[^a-zA-Z0-9 ]/, "").toLowerCase().split('\\s+')
}
