import static whelk.util.Jackson.mapper

KbSigel = ["https://libris.kb.se/library/S",
           "https://libris.kb.se/library/SRo",
           "https://libris.kb.se/library/NB",
           "https://libris.kb.se/library/SM",
           "https://libris.kb.se/library/SOT",
           "https://libris.kb.se/library/SB17",
           "https://libris.kb.se/library/NLT"]

propertiesToRemove = ["marc:shelvingScheme", "marc:shelvingOrder", "marc:groupid"]
moveIntoComponent = ["appliesTo", "location", "physicalLocation", "formerShelfLocation", "address", "codedLocationQualifier", "nonCodedLocationQualifier", "shelfMark", "availability", "shelfControlNumber", "callNumberPrefix", "shelfLabel", "callNumberSuffix", "country", "itemCondition", "copyrightArticleFeeCode", "copyNumber", "uri"]
moveOutOfComponent = ["cataloguersNote"]

String where = "collection = 'hold' AND data#>>'{@graph,1,heldBy,@id}' IN ( '£' )".replace("£", String.join("', '", KbSigel))

selectBySqlWhere(where) { data ->
    Map mainEntity = data.graph[1]

    boolean changed = false

    // Find the component to manipulate
    Map component = ["@type":"Item"]
    if (mainEntity.containsKey("hasComponent")) {
        if (! mainEntity.hasComponent instanceof List )
            mainEntity.hasComponent = [mainEntity.hasComponent]
        List components = (List) mainEntity.hasComponent
        if (!components.isEmpty())
            component = (Map) components[0]
    }
    mainEntity.put("hasComponent", [component])

    // Stuff in
    for (String property : moveIntoComponent) {
        def value = mainEntity[property]
        if (value != null) {
            component.put(property, value)
            changed = true
        }
        mainEntity.remove(property)
    }

    // Stuff out
    for (String property : moveOutOfComponent) {
        def value = component[property]
        if (value != null) {
            mainEntity.put(property, value)
            changed = true
        }
        component.remove(property)
    }

    // Stuff to remove
    for (String property : propertiesToRemove) {
        if (mainEntity.remove(property) != null)
            changed = true
        if (component.remove(property) != null)
            changed = true
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
