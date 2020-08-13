/*
 * if '{@graph ,1,@type}' = 'KitInstance' OR 'TextInstance'
 * if '{@graph ,1,hasPart,@type}' =  'KitInstance'
 * > @type = 'Instance'
 * (om det är den sista i listan ta även bort hasPart)
 * if '{@graph ,1,carrierType}' OR '{@graph ,1,hasPart,carrierType}'
 *
 * LIKE https://id.kb.se/marc/KitMaterialType-u
 * remove  'https://id.kb.se/marc/KitMaterialType-u'
 *
 * (om det är den sista i listan ta även bort carrierType)
 */

PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String where = "data#>>'{@graph,1,@type}' = 'KitInstance' or data#>>'{@graph,1,@type}' = 'TextInstance'"

selectBySqlWhere(where) { data ->
    def (record, instance, work) = data.graph
    boolean changed = false

    instance.hasPart.each { part ->
        if (part["@type"] == "KitInstance") {
            changed = true
            instance["@type"] = "Instance" // Inte part.type, visst?

            // Enda elementet i listan? Släng skiten!
            if (instance.hasPart.size == 1)
                instance.remove("hasPart")
        }
    }

    if (instance.carrierType) {
        changed |= cleanCarrierType(instance.carrierType)
        if (instance.carrierType.isEmpty())
            instance.remove("carrierType")
    }
    if (instance.hasPart) {
        instance.hasPart.each { part ->
            if (part.carrierType) {
                changed |= cleanCarrierType(part.carrierType)
                if (part.carrierType.isEmpty())
                    part.remove("carrierType")
            }
        }
    }

    if (work["@type"] == "Kit") {
        instance["@type"] = "Instance"
        changed = true
    }

    if (changed) {
        scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave(onError: { e ->
            failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
        })
    }
}

boolean cleanCarrierType(carrierTypeList) {
    def it = carrierTypeList.iterator()
    boolean changed = false
    while (it.hasNext()) {
        def typeEnt = it.next()
        if (typeEnt["@id"] == "https://id.kb.se/marc/KitMaterialType-u") {
            it.remove()
            changed = true
        }
    }
    return changed
}