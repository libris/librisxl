String whereBib = """
    collection = 'bib' 
    AND data#>>'{@graph,1,marc:hasImmediateSourceOfAcquisitionNote}' LIKE '%marc:accessionNumber%'
"""

selectBySqlWhere(whereBib) { data ->
    Map thing = data.graph[1]

    boolean modified

    asList(thing."marc:hasImmediateSourceOfAcquisitionNote").each {
        String accessionNumber = it.remove("marc:accessionNumber")
        if (accessionNumber) {
            Map accessionNumberEntity =
                    [
                            "@type": "AccessionNumber",
                            "value": "${accessionNumber}"
                    ]

            it["identifiedBy"] = [accessionNumberEntity]

            modified = true
        }
    }

    if (modified) {
        data.scheduleSave()
    }
}

String whereHold = """
    collection = 'hold'
    AND data#>>'{@graph,1,immediateAcquisition}' LIKE '%marc:accessionNumber%'
"""

selectBySqlWhere(whereHold) { data ->
    Map thing = data.graph[1]

    boolean modified

    asList(thing.immediateAcquisition).each {
        String accessionNumber = it.remove("marc:accessionNumber")

        if (accessionNumber) {
            Map accessionNumberEntity =
                    [
                            "@type": "AccessionNumber",
                            "value": "${accessionNumber}"
                    ]

            it["identifiedBy"] = [accessionNumberEntity]

            modified = true
        }
    }

    if (modified)
        data.scheduleSave()
}

List asList(o) {
    return o in List ? o : o != null ? [o] : []
}