String where = "collection='bib' and data#>>'{@graph,1,@type}' = 'TextInstance'"

selectBySqlWhere(where) { data ->
    boolean changed = false

    def instance = data.graph[1]

    if (instance["@type"] == "TextInstance") {
        instance["@type"] = "Print"
        changed = true
    }

    if (instance.hasPart) {
        for (Map part : instance.hasPart) {
            if (part["@type"] == "TextInstance") {
                if (part.carrierType) {
                    for (Map ct : part.carrierType) {
                        if (ct["@id"] == "https://id.kb.se/marc/RegularPrint") {
                            part["@type"] = "Print"
                            changed = true
                        }
                    }
                } else {
                    part["@type"] = "Instance"
                    changed = true
                }
            }
        }
    }

    if (changed) {
        data.scheduleSave()
    }
}