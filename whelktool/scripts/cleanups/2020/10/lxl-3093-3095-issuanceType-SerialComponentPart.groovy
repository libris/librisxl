/**
 * Change issuanceType SerialComponentPart to ComponentPart or Monograph
 *
 * See LXL-3093 and LXL-3095 for more info.
 */

PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String where = "collection = 'bib' AND deleted = false AND data#>>'{@graph,1,issuanceType}' = 'SerialComponentPart'"

selectBySqlWhere(where) { data ->
    def instance = data.graph[1]

    if (instance.issuanceType != "SerialComponentPart")
        return

    if (instance.isPartOf)
        instance.issuanceType = "ComponentPart"
    else
        instance.issuanceType = "Monograph"

    scheduledForUpdating.println("${data.doc.getURI()}")
    data.scheduleSave(onError: { e ->
        failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
    })
}
