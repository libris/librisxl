/**
 * Change imageBitDepth to digitalCharacteristic in instances where the value of imageBitDepth is
 * 008 or 024.
 *
 * See LXL-3184 for more info.
 */

PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String where = "collection = 'bib' AND deleted = false AND " +
        "(data#>>'{@graph,1,imageBitDepth}' IS NOT null OR data#>>'{@graph,1,hasPart}' LIKE '%imageBitDepth%')"

selectBySqlWhere(where) { data ->
    boolean changed = false
    def instance = data.graph[1]

    if (instance.imageBitDepth) {
        // Only copy imageBitDepth if value is 008 or 024; throw away the rest
        if (instance.imageBitDepth instanceof String && (instance.imageBitDepth == "008" || instance.imageBitDepth == "024")) {
            if (!instance.digitalCharacteristic) {
                instance.digitalCharacteristic = []
            } else if (!instance.digitalCharacteristic instanceof List) {
                instance.digitalCharacteristic = [instance.digitalCharacteristic]
            }

            instance.digitalCharacteristic << ["@type": "ImageBitDepth", "value": instance.imageBitDepth]
        }

        instance.remove("imageBitDepth")
        changed = true
    }

    // If there's any imageBitDepth in hasPart, get rid of it
    instance.hasPart?.each { part ->
        if (part.imageBitDepth) {
            part.remove("imageBitDepth")
            changed = true
        }
    }

    if (changed) {
        scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave(onError: { e ->
            failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
        })
    }
}
