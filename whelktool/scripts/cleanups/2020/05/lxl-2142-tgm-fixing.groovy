PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

/*

1. Bibposter ska länka till https://id.kb.se/term/gmgpc/swe/NÅNTING istället för https://id.kb.se/term/gmgpc%2F%2Fswe/NÅNTING

2. Alla https://id.kb.se/term/gmgpc%2F%2Fswe/NÅNTING termer ska få nya idn: https://id.kb.se/term/gmgpc/swe/NÅNTING

3. Alla termer i 2 ska också länka inScheme till exakt https://id.kb.se/term/gmgpc-swe istället för https://id.kb.se/term/gmgpc%2F%2Fswe

*/

// Update the terms
where = "data#>>'{@graph,1,inScheme,@id}' = 'https://id.kb.se/term/gmgpc%2F%2Fswe'"
selectBySqlWhere(where) { data ->

    data.graph[1].inScheme["@id"] = "https://id.kb.se/term/gmgpc-swe"

    if (! (data.graph[1]["sameAs"] instanceof List) ) {
        data.graph[1]["sameAs"] = []
    }
    data.graph[1]["sameAs"].add(data.graph[1]["@id"])
    data.graph[1]["@id"] = data.graph[1]["@id"].replace("%2F%2F", "/")

    scheduledForUpdating.println("${data.doc.getURI()}")
    data.scheduleSave(onError: { e ->
        failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
    })
}

// Update usages
where = "collection = 'bib' and data::text like '%https://id.kb.se/term/gmgpc%2F%2Fswe/%'"
selectBySqlWhere(where) { data ->
    def (record, instance, work) = data.graph

    boolean changed = false

    if (work.genreForm instanceof List) {
        work.genreForm.each {
            if (it["@id"] && it["@id"].startsWith("https://id.kb.se/term/gmgpc%2F%2Fswe/")) {
                it["@id"] = it["@id"].replace("%2F%2F", "/")
                changed = true
            }
        }
    }

    if (changed) {
        scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave(onError: { e ->
            failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
        })
    } else {
        failedUpdating.println("There seems to have been a gmgpc link in ${data.doc.shortId}, But found nothing to fix in @graph,2,genreForm.")
    }
}
