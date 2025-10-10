String where = "collection = 'hold' and data#>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/Ehs' and data#>'{@graph,1,hasComponent}' is not null"

selectBySqlWhere(where) { doc ->

    boolean changed = false

    if (doc.graph[1]?.hasComponent) {
        doc.graph[1]?.hasComponent.each { component ->
            if (component?.heldBy["@id"] != "https://libris.kb.se/library/Ehs") {
                component.heldBy["@id"] = "https://libris.kb.se/library/Ehs"
                changed = true
            }
        }
    }

    if (changed) {
        //System.err.println("Changed: " + doc.graph[1]?.hasComponent)
        doc.scheduleSave(loud: true)
    }
}
