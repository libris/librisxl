String where = "collection = 'bib' and data#>>'{@graph,1,identifiedBy}' like '%MTM medietyp%'"

selectBySqlWhere(where) { data ->

    // Find the two relevant identifiers
    List identifiedBys = asList(data.graph[1].identifiedBy)
    Map medieTypIdentifier = identifiedBys.find { identifiedBy ->
        identifiedBy["typeNote"] == "MTM medietyp"
    }
    Map medieNummerIdentifier = identifiedBys.find { identifiedBy ->
        identifiedBy["typeNote"] == "MTM medienummer"
    }
    if (medieNummerIdentifier == null || medieTypIdentifier == null || medieTypIdentifier.value == null)
        return

    // Make sure there's a qualifier list in medieNummerIdentifier to put stuff in
    if (medieNummerIdentifier.qualifier == null)
        medieNummerIdentifier.put("qualifier", [])
    if (! medieNummerIdentifier.qualifier instanceof List)
        medieNummerIdentifier.put("qualifier", [medieNummerIdentifier.qualifier])

    // Make the switch and drop the "medietyp"
    List qualifiers = medieNummerIdentifier.qualifier
    qualifiers.add(medieTypIdentifier.value)
    identifiedBys.remove(medieTypIdentifier)

    //System.err.println("ID by after change: " + identifiedBys)
    data.scheduleSave()
}

private List asList(Object o) {
    if (o == null)
        return []
    if (o instanceof List)
        return o
    return [o]
}
