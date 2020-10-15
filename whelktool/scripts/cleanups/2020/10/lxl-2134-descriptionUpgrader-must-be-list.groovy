/**
 * Turn descriptionUpgrader into a list.
 *
 * See LXL-2134 for more info.
 */

import whelk.util.DocumentUtil

PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String where = "data#>>'{@graph,0,descriptionUpgrader}' IS NOT NULL"

selectBySqlWhere(where) { data ->
    boolean changed = DocumentUtil.findKey(data.doc.data, 'descriptionUpgrader') { value, path ->
        if (value instanceof List) {
            return DocumentUtil.NOP
        }
        return new DocumentUtil.Replace([value])
    }

    if (changed) {
        scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave(onError: { e ->
            failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
        })
    }
}
