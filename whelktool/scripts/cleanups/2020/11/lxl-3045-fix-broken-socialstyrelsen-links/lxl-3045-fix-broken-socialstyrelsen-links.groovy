/**
 * Fix broken Socialstyrelsen publication links
 *
 * See LXL-3045 for more info.
 */

import java.text.SimpleDateFormat
import whelk.util.DocumentUtil

PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")
PrintWriter notUpdated = getReportWriter("not-updated")

Map newUris = [:]
List deadUris = []

new File(scriptDir, "socialstyrelsen_ny_uri.txt").eachWithIndex { row, index ->
    if (index == 0) return // Skip first line (column names)
    def (bibId, _title, _filename, oldUri, newUri) = row.split(/\t/).collect { it.trim() }
    newUris[bibId] = ["oldUri": oldUri, "newUri": newUri]
}

new File(scriptDir, "socialstyrelsen_flytta_uri.txt").eachWithIndex { row, index ->
    if (index == 0) return
    def (bibId, _title, _filename, oldUri) = row.trim().split(/\t/).collect { it.trim() }
    deadUris << bibId
}

// Documents that have a new URI: update the old one
selectByIds(newUris.keySet() as List) { data ->
    def instance = data.graph[1]
    def id = data.graph[0].sameAs[0]["@id"]

    boolean changed = DocumentUtil.findKey(instance, "uri") { value, path ->
        if (path[0] in ["associatedMedia", "marc:versionOfResource", "electronicLocator"]) {
            for (int i = 0; i < value.size(); ++i) {
                // startsWith because all old URIs in socialstyrelsen_ny_uri.txt have pdf at the end,
                // but the actual instances sometimes don't  -- specifically, seemingly the ones that
                // have the URI in marc:versionOfResource. They have e.g.
                // http://www.socialstyrelsen.se/publikationer2002/2002-123-24
                // instead of
                // http://www.socialstyrelsen.se/publikationer2002/2002-123-24.pdf
                if (newUris[id].oldUri.startsWith(value[i])) {
                    value[i] = newUris[id].newUri
                }
            }
            return new DocumentUtil.Replace(value)
        }
        return DocumentUtil.NOP
    }

    if (changed) {
        scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave(onError: { e ->
            failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
        })
    } else {
        notUpdated.println("${data.doc.getURI()}")
    }
}

// Documents that don't have a new URI: remove URI and add a note
selectByIds(deadUris) { data ->
    def instance = data.graph[1]
    String actualOldUri

    if (instance.associatedMedia?.size() == 1) {
        if (instance.associatedMedia[0].uri) {
            actualOldUri = instance.associatedMedia[0].uri[0]
        }
        instance.remove("associatedMedia")
    } else if (instance.electronicLocator?.size() == 1) {
        if (instance.electronicLocator[0].uri) {
            actualOldUri = instance.electronicLocator[0].uri[0]
        }
        instance.remove("electronicLocator")
    } else if (instance["marc:versionOfResource"] instanceof List && instance["marc:versionOfResource"].size() == 1) {
        if (instance["marc:versionOfResource"][0].uri) {
            actualOldUri = instance["marc:versionOfResource"][0].uri
        }
        instance.remove("marc:versionOfResource")
    } else if (instance["marc:versionOfResource"] instanceof Map) {
        if (instance["marc:versionOfResource"].uri) {
            actualOldUri = instance["marc:versionOfResource"].uri
        }
        instance.remove("marc:versionOfResource")
    }

    // If no matching old URI was found, do nothing but log for later processing
    if (!actualOldUri) {
        notUpdated.println("${data.doc.getURI()}")
        return
    }

    if (instance.hasNote == null)
        instance["hasNote"] = []
    else if (!(instance.hasNote instanceof List))
        instance["hasNote"] = [instance["hasNote"]]

    String noteText
    String currentDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
    if (instance['@type'] == "Electronic")
        noteText = "Tidigare fungerande URL. Socialstyrelsen tillhandahåller inte längre detta dokument. ${currentDate}"
    else
        noteText = "Tidigare fungerande URL för digital utgåva. Socialstyrelsen tillhandahåller inte längre detta dokument. ${currentDate}"
    instance.hasNote << ["@type": "Note", "label": "${actualOldUri} ${noteText}"]

    scheduledForUpdating.println("${data.doc.getURI()}")
    data.scheduleSave(onError: { e ->
        failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
    })
}
