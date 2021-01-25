PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String where = "data#>>'{@graph,1,@type}' = 'Geographic' and not data#>'{@graph,1}' @> " +
        "'{\"inScheme\":{\"@id\":\"https://id.kb.se/term/sao\"}}'"

selectBySqlWhere(where) { data ->

    Map thing = data.graph[1]

    thing['inScheme'] = ["@id": "https://id.kb.se/term/sao"]

    scheduledForUpdating.println("${data.doc.getURI()}")
    data.scheduleSave(onError: { e ->
        failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
    })
}