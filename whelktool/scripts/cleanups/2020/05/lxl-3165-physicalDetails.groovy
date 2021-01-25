PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String where = "data#>'{@graph,1,marc:otherPhysicalDetails}' is not null"

selectBySqlWhere(where) { data ->
    def (record, instance, work) = data.graph

    instance["physicalDetailsNote"] = instance["marc:otherPhysicalDetails"]
    instance.remove("marc:otherPhysicalDetails")

    scheduledForUpdating.println("${data.doc.getURI()}")
    data.scheduleSave(onError: { e ->
        failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
    })
}
