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
    data.graph[0]._marcUncompleted.each{ uncompleted ->
        if (uncompleted instanceof Map && uncompleted.keySet().contains("348"))
        {
            if ( uncompleted["348"].subfields instanceof List && ! (uncompleted["348"].subfields.isEmpty()) ) {
                uncompleted["348"].subfields.each { it ->
                    if (it["a"] != null) {
                        newLabel = it["a"]
                    }
                    if (it["2"] != null) {
                        newCode = it["2"]
                    }
                }
            }
        }
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
