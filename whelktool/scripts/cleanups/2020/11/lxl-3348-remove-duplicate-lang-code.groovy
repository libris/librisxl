PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String where =
        """data#>>'{@graph,1,instanceOf,language,0,@id}' = 'https://id.kb.se/language/und' 
        and data#>>'{@graph,1,instanceOf,language,1,@id}' = 'https://id.kb.se/language/swe' 
        and not data#>>'{@graph,0,bibliography,0,sigel}' = 'EPLK' 
        and deleted = false
        """

selectBySqlWhere(where) { data ->
    data.graph[1].instanceOf.language.remove(0)

    scheduledForUpdating.println("${data.doc.getURI()}")
    data.scheduleSave(onError: { e ->
        failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
    })
}


