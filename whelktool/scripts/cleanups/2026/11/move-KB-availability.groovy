import static whelk.util.Jackson.mapper

KbSigel = ["https://libris.kb.se/library/S",
           "https://libris.kb.se/library/SRo"]


String where = "collection = 'hold' AND data#>>'{@graph,1,heldBy,@id}' IN ( '£' )".replace("£", String.join("', '", KbSigel))

selectBySqlWhere(where) { data ->
    Map mainEntity = data.graph[1]

    boolean changed = false

    // Default "empty" new component
    Map primaryComponent = ["@type":"Item", "heldBy":mainEntity.get("heldBy")]

    // Find the primary component to manipulate
    if (mainEntity.containsKey("hasComponent")) {
        if (! mainEntity.hasComponent instanceof List )
            mainEntity.hasComponent = [mainEntity.hasComponent]
        List components = (List) mainEntity.hasComponent
        if (!components.isEmpty())
            primaryComponent = (Map) components[0]
        else
            components.add(primaryComponent)
    }
    else {
        //System.err.println(" FRESH component: " + mainEntity)
        mainEntity.put("hasComponent", [primaryComponent])
    }

    // Stuff out, this applies to all components
    for (String property : ["availability"]) { // Why right? Because this is an adaption of another script, with minimal changes
        for (Map component : (List) mainEntity.hasComponent) {

            if ( component.get("uri") != null && component.get("uri") instanceof String && ((String)component.get("uri")).contains("folio") ) {
                //System.err.println("Ignoring FOLIO written component.")
                continue
            }

            def value = component[property]
            if (value != null) {

                // Disregard linked ones
                if (isLinked(value)) {
                    //System.err.println(" LINKED (ignoring): " + value)
                    continue
                }

                if (!mainEntity.containsKey(property)) {
                    mainEntity.put(property, value)
                } else {
                    if (! (mainEntity.get(property) instanceof List)) {
                        mainEntity.put(property, [mainEntity.get(property)])
                    }
                    if (value instanceof List) {
                        for ( Object element : (List)value ) {
                            ((List) mainEntity.get(property)).add(element)
                            //System.err.println("After adding, before dedup: " + property + " now: " + mainEntity.get(property))
                        }
                    } else {
                        ((List) mainEntity.get(property)).add(value)
                    }
                    mainEntity.put(property, dedup(mainEntity.get(property)))
                    //System.err.println("After adding, after dedup: " + property + " now: " + mainEntity.get(property))
                }
                changed = true
            }
            component.remove(property)
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

boolean isLinked(Object availability) {
    if ( (availability instanceof List) ) {
        for (Object element : availability) {
            if (element instanceof Map) {
                if (element.get("@id") != null) {
                    return true
                }
            }
        }
    } else if (availability instanceof Map) {
        if (availability.get("@id") != null)
            return true
    }
    return false
}

List dedup(List l) {
    Set s = new HashSet(l)
    return new ArrayList(s)
}