String whereBib = """
    collection = 'bib' 
    AND data#>>'{@graph,1,marc:hasImmediateSourceOfAcquisitionNote}' LIKE '%marc:accessionNumber%'
"""

selectBySqlWhere(whereBib) { data ->
    Map thing = data.graph[1]
    List hasImmediateSourceOfAcquisitionNote = asList(thing."marc:hasImmediateSourceOfAcquisitionNote")

    boolean modified

    hasImmediateSourceOfAcquisitionNote.removeAll {
        String accessionNumber = it.remove("marc:accessionNumber")
        if (accessionNumber) {
            Map accessionNumberEntity =
                    [
                            "@type": "AccessionNumber",
                            "value": "${accessionNumber}"
                    ]

            thing.identifiedBy = thing.identifiedBy ?: []
            thing.identifiedBy << accessionNumberEntity

            modified = true

            // Remove if only {@type: marc:ImmediateSourceOfAcquisitionNote} left
            if (it.size() == 1)
                return true
        }
        return false
    }
    if (hasImmediateSourceOfAcquisitionNote.isEmpty())
        thing.remove("marc:hasImmediateSourceOfAcquisitionNote")

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
    List immediateAcquisition = asList(thing.immediateAcquisition)

    boolean modified

    immediateAcquisition.removeAll {
        String accessionNumber = it.remove("marc:accessionNumber")

        if (accessionNumber) {
            Map accessionNumberEntity =
                    [
                            "@type": "AccessionNumber",
                            "value": "${accessionNumber}"
                    ]

            thing.identifiedBy = thing.identifiedBy ?: []
            thing.identifiedBy << accessionNumberEntity

            modified = true

            // Remove if only {@type: ImmediateAcquisition} left
            if (it.size() == 1)
                return true
        }
        return false
    }

    if (immediateAcquisition.isEmpty())
        thing.remove("immediateAcquisition")

    if (modified)
        data.scheduleSave()
}

List asList(o) {
    return o in List ? o : o != null ? [o] : []
}