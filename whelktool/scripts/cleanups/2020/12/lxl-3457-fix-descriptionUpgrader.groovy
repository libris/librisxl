/**
 * Fix documents (possibly) messed up by a bug in lxl-2134-descriptionUpgrader-must-be-list.groovy
 *
 * See LXL-3457 for more info.
 */

import whelk.Document

PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String where = "data#>>'{@graph,0,descriptionUpgrader}' IS NOT NULL"

selectBySqlWhere(where) { data ->
    def record = data.graph[0]
    boolean changed = false

    // Gets all versions of the document and sorts them by 1) modified 2) generationDate,
    // returning a list with the oldest version first
    List<Document> versions = data.whelk.storage.loadAllVersions(data.doc.shortId).sort { a, b ->
        def (aModified, aGenerationDate) = [a.data["@graph"][0]["modified"], a.data["@graph"][0]["generationDate"]]
        def (bModified, bGenerationDate) = [b.data["@graph"][0]["modified"], b.data["@graph"][0]["generationDate"]]
        return aModified == bModified ? aGenerationDate <=> bGenerationDate : aModified <=> bModified
    }

    for (int i = 0; i < versions.size(); ++i) {
        if (!versions[i].data["@graph"][0].containsKey("generationProcess"))
            continue

        // If we find a version affected by the old broken script...
        if (versions[i].data["@graph"][0]["generationProcess"]["@id"] == "https://libris.kb.se/sys/globalchanges/cleanups/2020/10/lxl-2134-descriptionUpgrader-must-be-list.groovy") {
            // ...compare with the previous version
            if (i > 0 && record["descriptionUpgrader"] != asList(versions[i-1].data["@graph"][0]["descriptionUpgrader"])) {
                // Current version is broken, so use the previous version
                // (and at the same time make it a list if necessary)
                record["descriptionUpgrader"] = asList(versions[i-1].data["@graph"][0]["descriptionUpgrader"])
                changed = true
            }
            break
        }
    }

    if (changed) {
        scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave(onError: { e ->
            failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
        })
    }
}

private List asList(o) {
    (o instanceof List) ? (List) o : (o ? [o] : [])
}
