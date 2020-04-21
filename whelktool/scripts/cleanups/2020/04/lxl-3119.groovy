PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String where =
        "(\n" +
        "data#>>'{@graph,1,@type}' = 'Family' or\n" +
        "data#>>'{@graph,1,@type}' = 'Organization' or\n" +
        "data#>>'{@graph,1,@type}' = 'Jurisdiction' or\n" +
        "data#>>'{@graph,1,@type}' = 'Meeting' or\n" +
        "data#>>'{@graph,1,@type}' = 'Person' or\n" +
        "data#>>'{@graph,1,@type}' = 'Agent'\n" +
        ") and\n" +
        "(\n" +
        "data#>>'{@graph,1,hasBiographicalInformation}' is not null or\n" +
        "data#>>'{@graph,1,marc:hasBiographicalOrHistoricalData}' is not null or\n" +
        "data#>>'{@graph,1,hasHistoricalData}' is not null\n" +
        ")"

selectBySqlWhere(where) { data ->
    def (record, mainEntity) = data.graph

    boolean changed = false

    def keys = ["hasBiographicalInformation", "marc:hasBiographicalOrHistoricalData", "hasHistoricalData"]

    for (String key : keys) {

        Object obj = mainEntity.get(key)
        if (obj == null)
            continue

        if (obj instanceof List) {
            Iterator it = obj.iterator()
            while (it.hasNext()) {
                Object entity = it.next()
                if (transform(mainEntity, entity)) {
                    changed = true
                    it.remove()
                }
            }
            if ( obj.isEmpty() )
                mainEntity.remove(key)
        }

        else { // not a list
            if (transform(mainEntity, obj)) {
                changed = true
                mainEntity.remove(key)
            }
        }

    }

    if (changed) {
        scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave(onError: { e ->
        failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
        })
    }
}


boolean transform(Map mainEntity, Map entity) {
    String type = entity.get("@type")
    if (type == null)
        return false

    if (type == "BiographicalNote" ||
            type == "marc:BiographicalOrHistoricalData" ||
            type == "AdministrativeHistoryNote" ) {
        if (entity.size() == 2 && entity.containsKey("label")) {
            Object labels = entity.get("label")

            mainEntity.put("description", [])

            if (labels instanceof List)
                mainEntity.description.addAll(labels)
            else
                mainEntity.description.add(labels)

            if (mainEntity.description.isEmpty())
                mainEntity.remove("description")

            return true
        }
    }
    return false
}