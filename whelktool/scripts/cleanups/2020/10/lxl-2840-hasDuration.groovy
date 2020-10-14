PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String where = "collection = 'bib' and deleted = false and data#>>'{@graph,0,_marcUncompleted}' like '%306%'"

selectBySqlWhere(where) { data ->
    
    // Make sure "hasDuration" is a list
    def instance = data.graph[1]
    if (instance["hasDuration"] == null) {
        instance.put("hasDuration", [])
    } else if (!(instance["hasDuration"] instanceof List))
        instance.put("hasDuration", [instance["hasDuration"]])

    // Extract duration from marcUncompleted
    List<String> durations = []
    asList(data.graph[0]._marcUncompleted).each{ uncompleted ->
        if (uncompleted instanceof Map && uncompleted.keySet().contains("306"))
        {
            asList(uncompleted["306"].subfields).each { it ->
                if (it["a"] != null) {
                    durations.add(it["a"])
                }
            }
        }
    }
    if (durations.size() == 0)
        return

    // Construct new hasDuration entity
    def newHasDuration =
            [
                    "@type" : "Duration",
            ]
    if (durations.size() > 1) {
        newHasDuration["hasPart"] = []
        for (String duration : durations) {
            newHasDuration["hasPart"].add(
                    [
                            "@type" : "Duration",
                            "value" : duration
                    ])
        }
    } else {
        newHasDuration["value"] = durations[0]
    }

    // Clean up marcUncompleted
    data.graph[1]["hasDuration"].add(newHasDuration)
    for (int i = data.graph[0]._marcUncompleted.size() -1; i > -1; --i) {
        def field = data.graph[0]._marcUncompleted[i]
        if (field != null && field["306"] != null)
            data.graph[0]._marcUncompleted.remove(i)
    }

    scheduledForUpdating.println("${data.doc.getURI()}")
    data.scheduleSave(onError: { e ->
        failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
    })
}

private List asList(Object o) {
    if (o == null)
        return []
    if (o instanceof List)
        return o
    return [o]
}
