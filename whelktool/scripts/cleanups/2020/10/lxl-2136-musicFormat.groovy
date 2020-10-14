PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String where = "collection = 'bib' and deleted = false and data#>>'{@graph,0,_marcUncompleted}' LIKE  '%\"348\"%'"

selectBySqlWhere(where) { data ->

    // Make sure "musicFormat" is a list
    def instance = data.graph[1]
    if (instance["musicFormat"] == null) {
        instance.put("musicFormat", [])
    } else if (!(instance["musicFormat"] instanceof List))
        instance.put("musicFormat", [instance["musicFormat"]])

    // Extract label and code from _marcUncompleted
    String newLabel
    String newCode
    asList(data.graph[0]._marcUncompleted).each{ uncompleted ->
        if (uncompleted instanceof Map && uncompleted.keySet().contains("348"))
        {
            asList(uncompleted["348"].subfields).each { it ->
                if (it["a"] != null) {
                    newLabel = it["a"]
                }
                if (it["2"] != null) {
                    newCode = it["2"]
                }
            }
        }
    }

    // Clean up _marcUncompleted
    for (int i = data.graph[0]._marcUncompleted.size() -1; i > -1; --i) {
        def field = data.graph[0]._marcUncompleted[i]
        if (field != null && field["348"] != null)
            data.graph[0]._marcUncompleted.remove(i)
    }

    // Add MusicFormat entity
    if (newLabel != null && newCode != null) {
        HashMap newMusicFormat = [
                "@type":"MusicFormat",
                "label": [newLabel],
                "source":
                        [
                                "@type": "Source",
                                "code": newCode
                        ]
        ]
        instance["musicFormat"].add( newMusicFormat )

        scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave(onError: { e ->
            failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
        })
    }
}

private List asList(Object o) {
    if (o == null)
        return []
    if (o instanceof List)
        return o
    return [o]
}
