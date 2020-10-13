PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String where = "collection = 'bib' and deleted = false and data#>>'{@graph,0,_marcUncompleted}' LIKE  '%\"348\"%'"

selectBySqlWhere(where) { data ->

    def instance = data.graph[1]
    if (!instance["musicFormat"]) {
        instance.put("musicFormat", [])
    }

    String newLabel
    String newCode

    data.graph[0]._marcUncompleted.each{ uncompleted ->
        if (uncompleted["348"])
        {
            uncompleted["348"].subfields.each { it ->
                if (it["a"])
                    if (it["a"]) {
                        newLabel = it["a"]
                    }
                    if (it["2"]) {
                        newCode = it["2"]
                    }
            }
        }
    }

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
