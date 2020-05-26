PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

/*

_MÅSTE_ KÖRAS MED --skip-index !!

1. Alla https://id.kb.se/term/gmgpc%2F%2Fswe/NÅNTING termer ska få nya idn: https://id.kb.se/term/gmgpc/swe/NÅNTING

2. Bibposter ska länka till https://id.kb.se/term/gmgpc/swe/NÅNTING istället för https://id.kb.se/term/gmgpc%2F%2Fswe/NÅNTING
(detta sköter systemet implict iomed att 1 utförs).

3. Alla termer i 1 ska också länka inScheme till exakt https://id.kb.se/term/gmgpc/swe istället för https://id.kb.se/term/gmgpc%2F%2Fswe
(https://id.kb.se/term/gmgpc/swe finns inte ännu, men ska vara huvudID på samma term, efter definitionsändring)

*/

// Update the terms
where = "data#>>'{@graph,1,inScheme,@id}' = 'https://id.kb.se/term/gmgpc%2F%2Fswe'"
selectBySqlWhere(where) { data ->
    
    data.graph[1].inScheme["@id"] = "https://id.kb.se/term/gmgpc/swe"

    if (! (data.graph[1]["sameAs"] instanceof List) ) {
        data.graph[1]["sameAs"] = []
    }
    data.graph[1]["sameAs"].add( ["@id" : data.graph[1]["@id"]])
    data.graph[1]["@id"] = data.graph[1]["@id"].replace("%2F%2F", "/")

    scheduledForUpdating.println("${data.doc.getURI()}")
    data.scheduleSave(onError: { e ->
        failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
    })
}
