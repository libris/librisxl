String where = """
    collection = 'auth'
    AND data#>>'{@graph,0,_marcUncompleted}' LIKE '%\"751\"%'
"""

Map SUBFIELD_MAPPINGS =
        [
                "0": "marc:recordControlNumber",
                "a": "marc:geographicName",
                "x": "marc:generalSubdivision",
                "y": "marc:chronologicalSubdivision",
                "z": "marc:geographicSubdivision"
        ]

selectBySqlWhere(where) { data ->
    List uncompleted = data.graph[0]._marcUncompleted
    List addedEntryGeographicName = data.graph[1]."marc:hasAddedEntryGeographicName"

    List modifiedAegns = []

    uncompleted.removeAll { uc ->
        if (!uc."751")
            return false

        Map subfieldsAsMap = uc."751".subfields.collectEntries()

        // Each subfield should be one of 0,a,x,y,z
        if (!subfieldsAsMap.every { it.key in SUBFIELD_MAPPINGS.keySet() })
            return false

        // Find an object matching subfields a and/or 0
        Map aegnObj = addedEntryGeographicName.find { aegn ->
            subfieldsAsMap.subMap("a", "0").every { sf ->
                correspondingValue = aegn[SUBFIELD_MAPPINGS[sf.key]]
                return [sf.value] == asList(correspondingValue)
            }
        }

        // We must have an object to map to
        if (!aegnObj)
            return false

        // Remove the matched object before modifying so that we don't map to the same object twice
        addedEntryGeographicName.remove(aegnObj)

        // Map unhandled subfields
        subfieldsAsMap.subMap("x", "y", "z").each { k, v ->
            if (k in uc._unhandled) {
                aegnObj[SUBFIELD_MAPPINGS[k]] = asList(v)
                uc._unhandled.remove(k)
            }
        }

        // Map ind2 (always 0)
        assert uc."751".ind2 == "0"
        aegnObj["marc:thesaurus"] = "marc:LibraryOfCongressSubjectHeadingsNameAuthorityFile"

        modifiedAegns << aegnObj

        // Make sure nothing more needs to be taken care of before removing (ind1 is always undefined, so needs no attention)
        if (uc."751".every { it.key in ["ind1", "ind2", "subfields"] } && uc._unhandled.isEmpty())
            return true

        return false
    }

    modifiedAegns.each {
        addedEntryGeographicName << it
    }

    if (uncompleted.isEmpty())
        data.graph[0].remove("_marcUncompleted")

    if (modifiedAegns)
        data.scheduleSave()
}

List asList(Object o) {
    return o in List ? o : [o]
}