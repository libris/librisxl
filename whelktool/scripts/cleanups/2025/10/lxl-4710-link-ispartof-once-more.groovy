/**
 * Replace local isPartOf with link
*  Based on ../../2025/09/lxl-4676-link-ispartof-yet-again.groovy
 * See https://kbse.atlassian.net/browse/LXL-4710
 */

import java.util.regex.Matcher
import java.util.regex.Pattern
import whelk.util.Unicode

String where = """
    collection = 'bib' and deleted = false and data#>>'{@graph,1,isPartOf}' LIKE '%"controlNumber":%'
"""

skipped = getReportWriter("skipped")
info = getReportWriter("info")
def whelk = getWhelk()

//selectBySqlWhere(where) { doc ->
selectByIds(new File("ids.txt").readLines()) { doc ->
    def source_thing = doc.graph[1]
    def _logSkip = { msg -> skipped.println("${doc.doc.getURI()}: ${msg}") }
    def _logInfo = { msg -> info.println("Source: ${doc.doc.getURI()} ${msg}") }

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
        ["@type", "describedBy", "provisionActivityStatement"] as Set,
        ["@type", "hasTitle", "describedBy", "provisionActivityStatement"] as Set,
        ["@type", "hasTitle", "describedBy", "identifiedBy", "provisionActivityStatement"] as Set,
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
    }

    Map sourceProvision = null
    if (isPartOf.containsKey("provisionActivityStatement")) {
        if (!(isPartOf.provisionActivityStatement instanceof String)) {
            _logSkip("provisionActivityStatement not a String: ${isPartOf.provisionActivityStatement}")
            return
        }

        sourceProvision = Provision.parseProvisionActivityStatement(isPartOf.provisionActivityStatement)
        if (!sourceProvision) {
            _logSkip("could not parse provisionActivityStatement: ${isPartOf.provisionActivityStatement}")
            return
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

    if (isPartOf.containsKey("identifiedBy")) {
        List targetIdentifiers = getAtPath(targetThing, ['identifiedBy', '*'], [])

        List filteredIdentifiers = targetIdentifiers.findAll { it.containsKey("@type") && it.containsKey("value") && it["@type"] == sourceIdentifiedByType }
        if (filteredIdentifiers.size() == 0) {
            _logSkip("no identifier with type ${sourceIdentifiedByType} in target ${properUri}, probably LibrisIIINumber")
            return
        }
        if (filteredIdentifiers.size() > 1) {
            if (filteredIdentifiers.size() == 2 && sourceIdentifiedByType == "ISBN" && isSeeminglySameIdentifier(filteredIdentifiers[0]["value"], filteredIdentifiers[1]["value"], "ISBN")) {
                // This is OK!
            } else {
                def targetIdsString = filteredIdentifiers.collect { "'${it.value}'" }.join(', ')
                _logSkip("multiple identifiers in target ${properUri}. Type: ${sourceIdentifiedByType}, source: '${sourceIdentifiedByValue}', target: ${targetIdsString}")
                return
            }
        }
        if (!isSeeminglySameIdentifier(sourceIdentifiedByValue, filteredIdentifiers[0]["value"], sourceIdentifiedByType)) {
            _logSkip("identifiedBy.value mismatch: ${sourceIdentifiedByValue} in source, ${filteredIdentifiers[0]['value']} in target ${properUri}")
            return
        }
    }

    if (isPartOf.containsKey("provisionActivityStatement")) {
        List targetStartYears = getAtPath(targetThing, ['publication', '*', 'startYear'], [])
        List targetEndYears = getAtPath(targetThing, ['publication', '*', 'endYear'], [])
        List targetYears = getAtPath(targetThing, ['publication', '*', 'year'], [])
        List targetAgents = getAtPath(targetThing, ['publication', '*', 'agent', 'label'], []) + getAtPath(targetThing, ['publication', '*', 'hasPart', '*', 'agent', 'label'], []) + getAtPath(targetThing, ['hasTitle', '*', 'mainTitle'], [])
        List targetPlaces = getAtPath(targetThing, ['publication', '*', 'place', '*', 'label'], []) + getAtPath(targetThing, ['publication', '*', 'hasPart', '*', 'place', '*', 'label'], [])

        if (sourceProvision.isSerial) {
            if (!(targetStartYears.contains(sourceProvision.startYear))) {
                _logSkip("pAS startYear ${sourceProvision.startYear} not found in ${targetStartYears} in target ${properUri}")
                return
            }
            if (sourceProvision.endYear && !(targetEndYears.contains(sourceProvision.endYear))) {
                _logSkip("pAS endYear ${sourceProvision.endYear} not found in ${targetEndYears} in target ${properUri}")
                return
            }
        } else {
            if (!(targetYears.contains(sourceProvision.startYear))) {
                _logSkip("pAS year/startYear ${sourceProvision.startYear} not found in ${targetYears} in target ${properUri}")
                return
            }
        }

        if (sourceProvision.agent) {
            if (!(targetAgents.any { targetAgentLabel ->
                def (agentIsSame, _agentResult) = compareStrings(sourceProvision.agent, targetAgentLabel)
                return agentIsSame
            })) {
                _logSkip("pAS agent ${sourceProvision.agent} not found in ${targetAgents} in target ${properUri}")
                return
            }
        }

        if (sourceProvision.place) {
            if (!(targetPlaces.any { targetPlaceLabel ->
                def (placeIsSame, _placeResult) = compareStrings(sourceProvision.place, targetPlaceLabel)
                return placeIsSame
            })) {
                _logSkip("pAS place ${sourceProvision.place} not found in ${targetPlaces} in target ${properUri}")
                return
            }
        }
    }

    if (isPartOf.containsKey("hasTitle")) {
        def (isSameTitle, titleResult) = isSeeminglySameTitle(sourceTitle, targetThing.hasTitle, targetThing, properUri)
        if (!isSameTitle) {
            _logSkip("title mismatch: source: ${titleResult.source}, target: ${titleResult.target}, words not in target: ${titleResult.notInTarget}. Target ${properUri}")
            return
        } else {
            if (titleResult.info) {
                _logInfo(titleResult.info)
            }
        }
    }

    source_thing["isPartOf"][0].clear()
    source_thing["isPartOf"][0]["@id"] = properUri

    doc.scheduleSave()
}

class Provision {
    // provisionActivityStatement *USUALLY* follows a few specific patterns but there are
    // tons of small variations and corner cases, hence these ugly regexes.

    // Matches "Lund : Lund University Press, 1882-", " Uppsala : Informationsavdelningen, Uppsala universitet, 1976-1996", ..
    static final Pattern patternFull = Pattern.compile('^\\[?([\\p{L}\\s,\\.-]+)\\]?\\s?:\\s([\\p{L}0-9,\\.\\-&:\\(\\)\\[\\]\\/\'\\s]+),\\s(?:(?:Cop|cop)\\.\\s?)?\\[?(\\d{4})\\??\\]?(-\\d{4})?\\]?(-)?$')
    // Matches "1998-", "1970-1975", "1990-", ..
    static final Pattern patternYearsOnly = Pattern.compile('^(?:(?:Cop\\.?|cop\\.?|c\\.?)\\s?)?\\[?(\\d{4})\\??(-\\d{4})?(-)?(?:\\s?;)?\\,?\\.?\\]?(?:\\s?;)?(?:\\s?,)?$')
    // Matches "Stockholm, 1974", "Uppsala, 1990-1995", ..
    static final Pattern patternPlaceAndYear = Pattern.compile('^([\\p{L}\\s-,\'\\.]+), \\[?(\\d{4})\\??\\]?(-\\d{4})?\\]?(-)?$')

    static Map parseProvisionActivityStatement(String pa) {
        Matcher yearsOnlyMatcher = patternYearsOnly.matcher(pa.trim())
        if (yearsOnlyMatcher.matches()) {
            return [
                    startYear: yearsOnlyMatcher.group(1),
                    endYear: yearsOnlyMatcher.group(2) ? yearsOnlyMatcher.group(2).substring(1) : false,
                    isSerial: yearsOnlyMatcher.group(2) ? true : (yearsOnlyMatcher.group(3) ? true : false),
            ]
        }

        Matcher fullMatcher = patternFull.matcher(pa.trim())
        if (fullMatcher.matches()) {
            return [
                    place: fullMatcher.group(1).trim(),
                    agent: fullMatcher.group(2).trim(),
                    startYear: fullMatcher.group(3),
                    endYear: fullMatcher.group(4) ? fullMatcher.group(4).substring(1) : null,
                    isSerial: fullMatcher.group(4) ? true : (fullMatcher.group(5) ? true : false),
            ]
        }

        Matcher placeAndYearMatcher = patternPlaceAndYear.matcher(pa.trim())
        if (placeAndYearMatcher.matches()) {
            return [
                    place: placeAndYearMatcher.group(1).trim(),
                    startYear: placeAndYearMatcher.group(2),
                    endYear: placeAndYearMatcher.group(3) ? placeAndYearMatcher.group(3).substring(1) : null,
                    isSerial: placeAndYearMatcher.group(3) ? true : (placeAndYearMatcher.group(4) ? true : false),
            ]
        }

        return [:]
    }
}

static List compareStrings(String sourceString, String targetString) {
    boolean isMatch = false
    Set sourceStringWords = extractWords(sourceString) as Set
    Set targetStringWords = extractWords(targetString) as Set

    if (targetStringWords.containsAll(sourceStringWords)) {
        isMatch = true
    }

    return [
            isMatch,
            [
                source: sourceStringWords,
                target: targetStringWords,
                notInTarget: sourceStringWords - targetStringWords,
            ]
    ]
}

static boolean isSeeminglySameIdentifier(String sourceIdentifier, String targetIdentifier, String identifierType) {
    if (identifierType == "ISBN") {
        return compareIsbn(sourceIdentifier, targetIdentifier)
    } else if (identifierType == "ISSN") {
        return compareIssn(sourceIdentifier, targetIdentifier)
    } else {
        return sourceIdentifier.equalsIgnoreCase(targetIdentifier)
    }
}

static boolean compareIssn(String issn1, String issn2) {
    def cleaned1 = cleanIsbnIssn(issn1)
    def cleaned2 = cleanIsbnIssn(issn2)

    if (cleaned1.size() == 0 && cleaned2.size() == 0) {
        return true
    }

    // Do allow one missing digit
    if (cleaned1.size() < 7 || cleaned2.size() < 7) {
        return false
    }

    return Unicode.damerauLevenshteinDistance(cleaned1, cleaned2) < 2
}

static boolean compareIsbn(String isbn1, String isbn2) {
    def cleaned1 = cleanIsbnIssn(isbn1)
    def cleaned2 = cleanIsbnIssn(isbn2)

    if (cleaned1.size() == 0 && cleaned2.size() == 0) {
        return true
    }

    def normalized1 = normalizeIsbnForComparison(cleaned1)
    def normalized2 = normalizeIsbnForComparison(cleaned2)

    // Do allow one missing digit
    if (normalized1.size() < 9 || normalized2.size() < 9) {
        return false
    }

    return normalized1.equals(normalized2)
    // Don't do this here because ISBNs are sometimes sequential, so allowing an edit distance of
    // 1 would make us erroneously treat two distinct ISBNs as identical
    //return Unicode.damerauLevenshteinDistance(normalized1, normalized2) < 2
}

static String cleanIsbnIssn(String number) {
    return number.replaceAll(/\([^)]*\)/, '').replaceAll(/[^0-9Xx]/, '').toUpperCase()
}

static String normalizeIsbnForComparison(String isbn) {
    if (isbn.size() == 10) {
        return isbn[0..-2]
    } else if (isbn.size() == 13) {
        return isbn[3..-2]
    } else {
        return isbn
    }
}

static List isSeeminglySameTitle(String sourceTitle, List targetHasTitle, Map targetThing, String targetProperUri) {
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
    List wordsToRemove = [
        "ljudupptagning",
        "red",
        "av",
        "redaktor",
        "och",
        "by",
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
        "okober"
    ]
    sourceTitleWords.removeAll(wordsToRemove)

    Set targetTitleWords = new HashSet()
    targetTitles.each { targetTitleWords.addAll(extractWords(it)) }

    if (targetTitleWords.containsAll(sourceTitleWords)) {
        isMatch = true
    }

    String info

    if (!isMatch) {
        Set allTargetWords = new HashSet()
        extractValues(targetThing).each { allTargetWords.addAll(extractWords(it)) }
        if (allTargetWords.containsAll(sourceTitleWords)) {
            isMatch = true
        } else {
            def wordDiff = sourceTitleWords - allTargetWords
            def threshold = Math.max(1, (int) Math.ceil(sourceTitleWords.size() * 0.3))
            if (wordDiff.size() < threshold) {
                isMatch = true
            } else {
                info = "${sourceTitle}\nTarget: ${targetProperUri} ${targetHasTitle}\n${wordDiff.size()} source word(s) not anywhere in whole target record: ${wordDiff}\n\n"
            }
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
    return Unicode.removeAllDiacritics(title).replaceAll(/\//, " ").replaceAll("-", " ").replaceAll(/[^a-zA-Z0-9 ]/, "").toLowerCase().split('\\s+')
}

static List extractValues(thing) {
    def results = []

    def recurse
    recurse = { item ->
        if (item instanceof Map) {
            item.each { k, v ->
                recurse(v)
            }
        } else if (item instanceof List) {
            item.each { element ->
                recurse(element)
            }
        } else if (!(item instanceof Boolean)) {
            results << item
        }
    }

    recurse(thing)
    return results
}
