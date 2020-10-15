PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String where = "collection = 'bib' and data#>'{@graph,1,replaces}' @> '[{\"@type\":\"Serial\"}]'"

selectBySqlWhere(where) { data ->
    boolean changed = false

    data.graph[1]["replaces"].each { it ->
        if (it.containsKey("date")) {
            Object date = it["date"]

            // Missing frequency?
            if ( ! it.containsKey("frequency") ) {
                it.put("frequency", [ [
                                              "@type" : "Frequency",
                                              "label" : []
                                      ] ])
            }

            it["frequency"].each { freq ->
                freq.put("date", date)
                it.remove("date")
                changed = true
            }
        }
    }

    if (changed) {
        //System.out.println("efter: " + data.graph[1]["replaces"])
        scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave(onError: { e ->
            failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
        })
    }
}
