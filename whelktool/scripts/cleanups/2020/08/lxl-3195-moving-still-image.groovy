PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String where = "data#>>'{@graph,2,@type}' = 'MovingImage' and (data#>>'{@graph,2,classification,0,code}' like '%/BD' or data#>>'{@graph,2,classification,1,code}' like '%/BD') and deleted = false"

selectBySqlWhere(where) { data ->
    def (record, instance, work) = data.graph

    work["@type"] = "StillImage"
    scheduledForUpdating.println("${data.doc.getURI()}")

    data.scheduleSave(onError: { e ->
        failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
    })
}
