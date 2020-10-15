/**
 * Turn descriptionUpgrader into a list.
 *
 * See LXL-2134 for more info.
 */

PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String where = "data#>>'{@graph,0,descriptionUpgrader}' IS NOT NULL"

selectBySqlWhere(where) { data ->
    record = data.graph[0]

    if (record["descriptionUpgrader"] instanceof List) {
        return
    } else if (record["descriptionUpgrader"] == null) {
        record["descriptionUpgrader"] = []
    } else {
        record["descriptionUpgrader"] = [record["descriptionUpgrader"]]
    }

    scheduledForUpdating.println("${data.doc.getURI()}")
    data.scheduleSave(onError: { e ->
        failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
    })
}
