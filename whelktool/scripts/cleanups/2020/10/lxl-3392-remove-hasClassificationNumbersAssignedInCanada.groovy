/**
 * Remove marc:hasClassificationNumbersAssignedInCanada from works.
 *
 * See LXL-3392 for more info.
 */

PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String where = "collection = 'bib' AND deleted = false AND data#>'{@graph,1,instanceOf}' ?? 'marc:hasClassificationNumbersAssignedInCanada'"

selectBySqlWhere(where) { data ->
    def instance = data.graph[1]

    if (instance.instanceOf && instance.instanceOf["marc:hasClassificationNumbersAssignedInCanada"]) {
        instance.instanceOf.remove("marc:hasClassificationNumbersAssignedInCanada")

        scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave(onError: { e ->
            failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
        })
    }
}
