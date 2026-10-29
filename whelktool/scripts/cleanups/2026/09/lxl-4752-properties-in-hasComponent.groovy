import static whelk.util.Jackson.mapper

KbSigel = ["https://libris.kb.se/library/S",
           "https://libris.kb.se/library/SRo",
           "https://libris.kb.se/library/NB",
           "https://libris.kb.se/library/SM",
           "https://libris.kb.se/library/SOT",
           "https://libris.kb.se/library/SB17",
           "https://libris.kb.se/library/NLT"]

propertiesToRemove = ["marc:shelvingScheme", "marc:shelvingOrder", "marc:groupid", "marc:acquisitionMethod", "marc:shelvingOrder", "inventoryLevel", "organizationalUnit"]
moveIntoComponent = ["appliesTo", "location", "physicalLocation", "formerShelfLocation", "address", "codedLocationQualifier", "nonCodedLocationQualifier", "shelfMark", "availability", "shelfControlNumber", "callNumberPrefix", "shelfLabel", "callNumberSuffix", "country", "itemCondition", "copyrightArticleFeeCode", "copyNumber", "uri"]
moveToHasNoteWithShelfmarkPrefix = ["marc:pieceDesignation"]
moveToHasNote = ["marc:publicNote", "marc:itemCondition"]
moveOutOfComponent = ["cataloguersNote", "hasNote", "marc:hasItemInformationBasicBibliographicUnit", "marc:hasTextualHoldingsBasicBibliographicUnit", "genreForm", "immediateAcquisition", "summary", "marc:copyIdentification", "marc:hasCopyAndVersionIdentificationNote", "custodialHistory"] + moveToHasNote

String where = "collection = 'hold' AND data#>>'{@graph,1,heldBy,@id}' IN ( '£' )".replace("£", String.join("', '", KbSigel))

selectBySqlWhere(where) { data ->
    Map mainEntity = data.graph[1]

    boolean changed = false

    // Find the primary component to manipulate
    Map primaryComponent = ["@type":"Item"]
    if (mainEntity.containsKey("hasComponent")) {
        if (! mainEntity.hasComponent instanceof List )
            mainEntity.hasComponent = [mainEntity.hasComponent]
        List components = (List) mainEntity.hasComponent
        if (!components.isEmpty())
            primaryComponent = (Map) components[0]
        else
            components.add(primaryComponent)
    }
    else
        mainEntity.put("hasComponent", [primaryComponent])

    // Stuff in (-to primary component only)
    for (String property : moveIntoComponent) {
        def value = mainEntity[property]
        if (value != null) {
            primaryComponent.put(property, value)
            changed = true
        }
        mainEntity.remove(property)
    }

    // Stuff from component to hasNote paired with shelfControlNumber.
    for (String property : moveToHasNoteWithShelfmarkPrefix) {
        for (Map component : (List) mainEntity.hasComponent) {
            def value = component[property]
            if (value != null) {
                def scn = component["shelfControlNumber"]

                // make sure hasNote is there and a list
                if (!mainEntity.containsKey("hasNote"))
                    mainEntity.put("hasNote", [])
                if (!mainEntity["hasNote"] instanceof List)
                    mainEntity.put("hasNote", [mainEntity["hasNote"]])
                List notes = mainEntity["hasNote"]

                if (scn == null) {
                    notes.add(value)
                } else {
                    if (scn instanceof List)
                        scn = scn[0]
                    notes.add(scn + ": " + value)
                }

                //System.err.println(property + ": " + value + " to hasNote paired with " + scn + ". Result: " + mainEntity["hasNote"] + " from " + mainEntity["@id"])
                component.remove(property)
                changed = true
            }
        }
    }

    // Stuff out, this applies to all components
    for (String property : moveOutOfComponent) {
        for (Map component : (List) mainEntity.hasComponent) {
            def value = component[property]
            if (value != null) {
                if (!mainEntity.containsKey(property)) {
                    mainEntity.put(property, value)
                } else {
                    if (! (mainEntity.get(property) instanceof List)) {
                        mainEntity.put(property, [mainEntity.get(property)])
                    }
                    ( (List)mainEntity.get(property) ).add(value)
                    mainEntity.put(property, dedup(mainEntity.get(property)))
                }
                changed = true
            }
            component.remove(property)
        }
    }

    // Stuff to hasNote (these have already been moved out of the components, if they were there)
    for (String property : moveToHasNote) {
        if (mainEntity.containsKey(property)) {
            def value = mainEntity[property]

            // make sure hasNote is there and a list
            if (!mainEntity.containsKey("hasNote"))
                mainEntity.put("hasNote", [])
            if (!mainEntity["hasNote"] instanceof List)
                mainEntity.put("hasNote", [mainEntity["hasNote"]])
            List notes = mainEntity["hasNote"]

            if (value instanceof List) {
                for (String s : value) {
                    notes.add(s)
                }
            } else {
                notes.add(value)
            }

            mainEntity.remove(property)
            changed = true

            //System.err.println(" Moved " + property + " to hasNote. Result: " + mainEntity["hasNote"] + " from " + mainEntity["@id"])
        }
    }

    // Stuff to remove, applies to all components
    for (String property : propertiesToRemove) {
        if (mainEntity.remove(property) != null)
            changed = true
        for (Map component : (List) mainEntity.hasComponent) {
            if (component.remove(property) != null)
                changed = true
        }
    }

    // No empty hasComponent!
    if (mainEntity.hasComponent.equals([Map.of("@type", "Item")])) {
        mainEntity.remove("hasComponent")
    }

    if (changed) {
        //System.err.println("Item with hasComponent:\n" + mapper.writeValueAsString(mainEntity))
        data.scheduleSave()
    }
}

List dedup(List l) {
    Set s = new HashSet(l)
    return new ArrayList(s)
}