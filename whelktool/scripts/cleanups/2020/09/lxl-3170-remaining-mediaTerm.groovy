PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")
PrintWriter needsManualReview = getReportWriter("remaining-records")

String where = "collection='bib' and (" +
        "data#>>'{@graph,1,marc:mediaTerm}' like '%/%' or " +
        "data#>>'{@graph,1,marc:mediaTerm}' like '%=%' or " +
        "data#>>'{@graph,1,marc:mediaTerm}' like '%;%' " +
        ")"

selectBySqlWhere(where) { data ->
    boolean changed = false

    Map instance = data.graph[1]
    String mediaTerm = instance["marc:mediaTerm"]
    mediaTerm = balanceBrackets(mediaTerm)

    int slashIndex = mediaTerm.indexOf('/')
    if (slashIndex != -1 && instance["responsibilityStatement"] == null) {
        instance["responsibilityStatement"] = mediaTerm.substring(slashIndex+1)
        mediaTerm = mediaTerm.substring(0, slashIndex)
        changed = true
    }

    int equalsIndex = mediaTerm.indexOf('=')
    if (equalsIndex != -1) {
        String parallelTitle = mediaTerm.substring(equalsIndex+1).trim()
        if (instance["hasTitle"] == null)
            instance["hasTitle"] = []
        if ( !(instance["hasTitle"] instanceof List) )
            instance["hasTitle"] = [instance["hasTitle"]]
        instance["hasTitle"].add(["@type": "ParallelTitle", "mainTitle": parallelTitle])
        mediaTerm = mediaTerm.substring(0, equalsIndex)
        changed = true
    }

    int semiColonIndex = mediaTerm.indexOf(';')
    if (semiColonIndex != -1 && instance["hasTitle"] instanceof List) {
        String titleRemainder = mediaTerm.substring(semiColonIndex).trim()
        for (Map title : instance["hasTitle"]) {
            if (title["@type"] == "Title" && title["mainTitle"]) {
                title["mainTitle"] = title["mainTitle"] + titleRemainder
                mediaTerm = mediaTerm.substring(0, semiColonIndex)
                changed = true
                break
            }
        }
    }

    if (changed) {
        instance["marc:mediaTerm"] = mediaTerm
        scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave(onError: { e ->
            failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
        })
    } else {
        needsManualReview.println("${data.doc.getURI()}")
    }
}

/**
 1. Remove closing brackets that were never opened.
 2. Add closing brackets at EOL to match number of still open brackets
 */
String balanceBrackets(String unbalanced) {
    StringBuilder result = new StringBuilder(unbalanced.length() + 5)
    int level = 0
    for (int i = 0; i < unbalanced.length(); ++i) {
        char charAtI = unbalanced.charAt(i)
        if (charAtI == '[')
            ++level
        else if (charAtI == ']')
            --level

        if (level < 0)
            level = 0
        else if (!Character.isWhitespace(charAtI))
            result.append(charAtI)
    }

    for (int i = 0; i < level; ++i)
        result.append(']')

    return result.toString()
}
