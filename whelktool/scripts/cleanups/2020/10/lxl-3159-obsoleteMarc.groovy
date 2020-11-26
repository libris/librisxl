PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

where = "collection = 'hold' and (data#>'{@graph,0,marc:entryMap}' is not null or data#>'{@graph,0,marc:characterCoding}' is not null)"

selectBySqlWhere(where) { data ->

    def adminmd = data.graph[0]

    adminmd.remove('marc:entryMap')
    adminmd.remove('marc:characterCoding')


    scheduledForUpdating.println("${data.doc.getURI()}")
    data.scheduleSave(onError: { e ->
        failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
    })

}