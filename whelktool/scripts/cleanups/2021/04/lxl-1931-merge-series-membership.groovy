/**
 * Merge split SeriesMembership entities.
 *
 * Match on properties that correspond to marc subfields $a $v $x in 490 and 830 respectively
 * $a have to match, plus either $v or $x given that the other also match or is non-existent
 * We compare only strings
 *
 * $a (mainTitle or seriesStatement) is considered a match if all words match (although we allow minor differences when comparing words)
 * $v (seriesEnumeration) if the whole strings match (except trailing periods and leading zeros in numeric values)
 * $x (issn) if all numeric values match
 *
 * See LXL-1931 for more info
 *
 */

import java.text.Normalizer
import whelk.Document

PrintWriter multipleMatches = getReportWriter("multiple-matches.txt")

String where = "collection = 'bib' AND data#>'{@graph,1,seriesMembership,1}' IS NOT NULL"

selectBySqlWhere(where) { data ->
    String id = data.doc.shortId

    List seriesMembership = data.graph[1]["seriesMembership"]

    int initialSize = seriesMembership.size()

    //Remove exact duplicates
    seriesMembership.unique()

    Map cmpData = seriesMembership.collectEntries {
        [it, createCmpMap(it)]
    }.findAll {
        it.value
    }

    List matchingPairs = cmpData.keySet().toList().subsequences().findAll {
        it.size() == 2 && matchProperties(cmpData[it[0]].leavesForCmp, cmpData[it[1]].leavesForCmp)
    }.toList()


    // There can be multiple matches, count how many times each object is paired
    Map matchCount = matchingPairs.flatten().countBy { it }

    boolean multiMatch = matchingPairs.removeAll {
        matchCount[it[0]] > 1 || matchCount[it[1]] > 1
    }

    // Save a report of ids with multiple matches, needs a closer look
    if (multiMatch)
        multipleMatches.println(id)

    matchingPairs.each {
        Map a = cmpData[it[0]]
        Map b = cmpData[it[1]]

        Map mergedLeaves = mergeLeaves(a.leavesForCmp, b.leavesForCmp, b.tree.inSeries?.instanceOf.asBoolean())
        Map mergedTrees = mergeTrees(a.tree, b.tree)

        // If merging failed due to conflicting values, abort
        if (!mergedTrees)
            return

        seriesMembership.remove(it[0])
        seriesMembership.remove(it[1])

        // Put the earlier removed properties back in place
        Map mergedSM = rebuild(mergedTrees, mergedLeaves)

        seriesMembership << mergedSM
    }

    if (initialSize > seriesMembership.size()) {
        data.scheduleSave()
    }
}

Map createCmpMap(Map sm) {
    Map smCopy = Document.deepCopy(sm)

    Map leavesForCmp = [:]

    List paths =
            [
                    ["seriesEnumeration"],
                    ["seriesStatement"],
                    ["inSeries", "identifiedBy"],
                    ["inSeries", "identifiedBy", "value"],
                    ["inSeries", "instanceOf"],
                    ["inSeries", "instanceOf", "hasTitle"],
                    ["inSeries", "instanceOf", "hasTitle", "mainTitle"]
            ]

    Set leaves = ["seriesEnumeration", "seriesStatement", "value", "mainTitle"] as Set

    // Get property values for comparison (and remove them to facilitate merging later on)
    // Don't return anything if there are multiple values in any of the paths (we want only 1-1 comparison)
    // Single element lists -> replace the list with its content (also to facilitate merging)
    for (path in paths) {
        def step = smCopy
        def next = path.pop()
        while (!path.isEmpty() && step[next] != null) {
            step = step[next]
            next = path.pop()
        }
        def value = step[next]
        if (value) {
            if (value instanceof List) {
                if (value.size() > 1) {
                    return
                }
                step[next] = value[0]
            }
            if (next in leaves)
                leavesForCmp[next] = step.remove(next)
        }
    }

    return ["leavesForCmp": leavesForCmp, "tree": smCopy]
}

Map mergeTrees(Map a, Map b) {
    for (entry in b) {
        def key = entry.key
        def val = entry.value
        if ((a[key] instanceof String || a[key] instanceof List) && a[key] != val) {
            // We don't allow conflicting strings or lists
            return
        } else if (a[key] instanceof Map) {
            a[key] = mergeTrees(a[key], val)
            if (!a[key]) {
                return
            }
        } else
            a[key] = val
    }
    return a
}

Map mergeLeaves(Map a, Map b, boolean bTreeHasWork) {
    b.each { key, val ->
        if (!a[key] || (key == "seriesEnumeration" && !bTreeHasWork) || (key == "value" && bTreeHasWork))
            a[key] = val
    }
    return a
}

Map rebuild(Map tree, Map leaves) {
    leaves.each { prop, val ->
        if (prop == "seriesStatement")
            tree.seriesStatement = val
        else if (prop == "seriesEnumeration")
            tree.seriesEnumeration = val
        else if (prop == "mainTitle") {
            Map hasTitle = tree.inSeries.instanceOf.hasTitle
            hasTitle.mainTitle = val
            // Make hasTitle a list like it was originally
            tree.inSeries.instanceOf.hasTitle = [hasTitle]
        } else if (prop == "value") {
            Map identifiedBy = tree.inSeries.identifiedBy
            identifiedBy["value"] = val
            // Make identifiedBy a list like it was originally
            tree.inSeries.identifiedBy = [identifiedBy]
        }
    }

    return tree
}

boolean matchProperties(Map a, Map b) {
    boolean titlesMatch = matchTitles(a.subMap("mainTitle", "seriesStatement"),
            b.subMap("mainTitle", "seriesStatement"))
    def sEnumMatch = a.seriesEnumeration && b.seriesEnumeration
            ? matchSeriesEnum(a.seriesEnumeration, b.seriesEnumeration)
            : null
    def issnMatch = a["value"] && b["value"]
            ? matchIssn(a["value"], b["value"])
            : null

    List accepted = [[true, true, true], [true, null, true], [true, true, null]]

    if ([titlesMatch, sEnumMatch, issnMatch] in accepted)
        return true

    return false
}

boolean matchTitles(Map a, Map b) {
    if (a.isEmpty() || b.isEmpty())
        return false
    // The usual case, where we compare mainTitle and seriesStatement
    if (a.size() == 1 && b.size() == 1)
        return matchStrings(a.values()[0], b.values()[0])
    // If there are properties common to both parts (e.g. both have mainTitle), compare them instead
    return a.every { key, val ->
        if (b[key])
            return matchStrings(val, b[key])
        else
            return true
    }
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
    return trimSeriesEnum(se1) == trimSeriesEnum(se2)
}

boolean matchIssn(String issn1, String issn2) {
    return getNumbers(issn1) == getNumbers(issn2)
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

List getWords(String s) {
    List words = s.split(/(:|\/|,|-| (& |and )?|\.|;|\[|\]|\(.*\))+/)
    if (words[0] == "")
        words.remove(0)
    return words
}

List getNumbers(String s) {
    List numbers = s.split(/\D+/)
    if (numbers[0] == "")
        numbers.remove(0)
    return numbers
}

String trimSeriesEnum(String se) {
    return se.replaceAll(/\.$|(?<!\d)0+/, "")
}
