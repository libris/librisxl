/**
 * Merge split SeriesMembership entities.
 *
 * Match on properties that correspond to marc subfields $a $v $x in 490 and 830 respectively
 * $a have to match, plus either $v or $x given that the other also match or is non-existent
 * We compare only strings
 *
 * $a is considered a match if all words match (although we allow minor differences when comparing words)
 * $v if all numeric value match
 * $x if all numeric value match
 *
 * See LXL-1931 for more info
 *
 */

import java.text.Normalizer

PrintWriter multipleMatches = getReportWriter("multiple-matches.txt")

String where = "collection = 'bib' AND data#>'{@graph,1,seriesMembership,1}' IS NOT NULL"

selectBySqlWhere(where) { data ->
    String id = data.doc.shortId

    List seriesMembership = data.graph[1]["seriesMembership"]

    // Skip if any of the concerned properties have multiple values, we want only 1-1 comparison
    // Also skip if any single value has the wrong type, e.g. "TitlePart" instead of "Title"
    if (seriesMembership.any { multipleValuesOrWrongType(it) })
        return

    Map cmpMap = seriesMembership.collectEntries {
        [it, getRelevantProps(it)]
    }

    List matchingPairs = seriesMembership.subsequences().findAll {
        it.size() == 2 && isMatchingPair(cmpMap[it[0]], cmpMap[it[1]])
    }.toList()

    // There can be multiple matches, count how many times each object is paired
    Map matchCount = matchingPairs.flatten().countBy { it }

    boolean multiMatch = matchingPairs.removeAll {
        matchCount[it[0]] > 1 || matchCount[it[1]] > 1
    }

    if (multiMatch)
        multipleMatches.println(id)

    matchingPairs.each {
        seriesMembership.remove(it[0])
        seriesMembership.remove(it[1])
        seriesMembership << it[0] + it[1]
    }

    if (!matchingPairs.isEmpty())
        data.scheduleSave()
}

boolean isMatchingPair(Map part, Map counterpart) {

    boolean titlesMatch = matchTitles(part.titles, counterpart.titles)
    boolean sEnumMatch = part.sEnum && counterpart.sEnum
            ? matchSeriesEnum(part.sEnum, counterpart.sEnum)
            : null
    boolean issnMatch = part.issn && counterpart.issn
            ? matchIssn(part.issn, counterpart.issn)
            : null

    List accepted = [[true, true, true], [true, null, true], [true, true, null]]

    if ([titlesMatch, sEnumMatch, issnMatch] in accepted)
        return true

    return false
}

boolean matchTitles(Map part, Map counterpart) {
    List comparablePairs = [part, counterpart].combinations().findAll { c, cp -> c.value != null && cp.value != null}

    // The usual case, where we compare mainTitle and seriesStatement
    if (comparablePairs.size() == 1) {
        List onlyComparablePair = comparablePairs[0]
        return matchStrings(onlyComparablePair[0].value, onlyComparablePair[1].value)
    }
    // If there are properties common to both parts (e.g. both have mainTitle), compare them instead
    if (comparablePairs.size() > 1)
        return comparablePairs.findAll{c, cp -> c.key == cp.key}.every {c, cp -> matchStrings(c.value, cp.value)}

    return false
}

Map getRelevantProps(Map sm) {
    Map props = [:]

    props["titles"] =
            ["mainTitle" : asList(asList(sm.inSeries?.instanceOf)[0]?.hasTitle?.first()?.mainTitle)[0],
             "sStatement": asList(sm.seriesStatement)[0]]
    props["sEnum"] = asList(sm.seriesEnumeration)[0]
    props["issn"] = sm.inSeries?.identifiedBy?.first()?.value

    return props
}

boolean multipleValuesOrWrongType(Map sm) {
    List values =
            [
                    sm.seriesEnumeration,
                    sm.seriesStatement,
                    sm.inSeries?.instanceOf,
                    sm.inSeries?.identifiedBy,
                    asList(sm.inSeries?.instanceOf)[0]?.hasTitle?.first()?.mainTitle
            ]

    if (values.any { it instanceof List && it.size() > 1 })
        return true

    return !(sm.inSeries?.identifiedBy?.first()?."@type" in ["ISSN", null]
            && asList(sm.inSeries?.instanceOf)[0]?.hasTitle?.first()?."@type" in ["Title", null])
}

boolean matchStrings(String a, String b) {
    if (a == b)
        return true

    List aWords = normalize(a)
    List bWords = normalize(b)

    if (aWords == bWords)
        return true

    boolean match = true
    int aLength = aWords.size()
    int bLength = bWords.size()
    int i = 0

    // Compare the string, word for word
    // Accept spacing between words, e.g. "super hero" = "superhero"
    // Require an exact match when comparing concatenated (e.g. "för färdiga" != "förfärliga")
    while (match && i < Math.min(aLength, bLength)) {
        if (matchWords(aWords[i], bWords[i]))
            i += 1
        else if (i + 1 < aLength && aWords[i] + aWords[i + 1] == bWords[i]) {
            aWords.remove(i + 1)
            aLength -= 1
            i += 1
        } else if (i + 1 < bLength && aWords[i] == bWords[i] + bWords[i + 1]) {
            bWords.remove(i + 1)
            bLength -= 1
            i += 1
        } else
            match = false
    }

    // All words match and the number of words are the same ("super hero" counts as one word if matched with "superhero")
    if (match && aLength == bLength)
        return true

    return false
}

boolean matchSeriesEnum(String se1, String se2) {
    if (se1 == se2)
        return true

    List se1numbers = getNumbers(se1).collect { it.replaceFirst(/^0+/, "") }
    List se2numbers = getNumbers(se2).collect { it.replaceFirst(/^0+/, "") }

    if (se1numbers.isEmpty() || se2numbers.isEmpty())
        return false

    if (se1numbers == se2numbers)
        return true

    return false
}

boolean matchIssn(String issn1, String issn2) {
    if (issn1 == issn2)
        return true

    List issn1numbers = getNumbers(issn1)
    List issn2numbers = getNumbers(issn2)

    if (issn1numbers == issn2numbers)
        return true

    return false
}

/**
 Calculate the levenshtein distance and use this value to decide whether the strings are a good enough match
 */
boolean matchWords(String w1, String w2) {
    int w1length = w1.size()
    int w2length = w2.size()

    int rows = w1length + 1
    int cols = w2length + 1

    List<List> distMatrix = []

    for (i in 0..<rows) {
        distMatrix << []
        for (j in 0..<cols)
            distMatrix[i] << 0
    }

    for (i in 0..<rows) {
        for (j in 0..<cols) {
            if (i == 0)
                distMatrix[i][j] = j
            else if (j == 0)
                distMatrix[i][j] = i
            else if (w1[i - 1] == w2[j - 1]) {
                distMatrix[i][j] = distMatrix[i - 1][j - 1]
            } else {
                distMatrix[i][j] = 1 + [distMatrix[i][j - 1],
                                        distMatrix[i - 1][j],
                                        distMatrix[i - 1][j - 1]].min()
            }
        }
    }


    int editDist = distMatrix.last().last()
    // The edit distance doesn't consider string lengths
    // E.g. "hej" and "bra" gets the same value (3) as "hej" and "hejsan"
    // So we need to normalize to get a good metric
    // The normalized value for "hej" and "bra" is 0, and for "hej" and "hejsan" it's 1/2
    int longest = Math.max(w1length, w2length)
    def normalizedDist = (longest - editDist) / longest

    // Max value is 1 (exact match) but we consider 3/4 good enough
    if (normalizedDist >= 3 / 4)
        return true

    return false
}


List normalize(String s) {
    String lowerCase = s.toLowerCase()
    String stripped = asciiFold(lowerCase)
    List words = getWords(stripped)
    if (words[0] in ["the", "il", "el", "le", "la", "die", "das"])
        words.remove(0)
    return words
}

String asciiFold(String s) {
    return Normalizer.normalize(s, Normalizer.Form.NFD).replaceAll('\\p{M}', '')
}

List getWords(String str) {
    List words = str.split(/(:|\/|,|-| (& |and )?|\.|;|\[|\]|\(.*\))+/)
    if (words[0] == "")
        words.remove(0)
    return words
}

List getNumbers(String str) {
    List numbers = str.split(/\D+/)
    if (numbers[0] == "")
        numbers.remove(0)
    return numbers
}

List asList(Object o) {
    return o instanceof List ? o : [o]
}
