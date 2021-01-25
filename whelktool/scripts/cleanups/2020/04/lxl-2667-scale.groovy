PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String where = "data#>'{@graph,1,scale}' is not null"

selectBySqlWhere(where) { data ->
    def (record, instance) = data.graph

    if ((instance.scale instanceof String)) {
        instance.scale = ["label": instance.scale, "@type": "Scale"]

        scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave(onError: { e ->
            failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
        })
    }
}
