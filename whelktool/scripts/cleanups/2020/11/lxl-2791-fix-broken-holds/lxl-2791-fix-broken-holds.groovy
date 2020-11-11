/**
 * Fix or remove holds that aren't associated with a bib record.
 *
 * See LXL-2791 for more info.
 */

PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")
PrintWriter scheduledForDeletion = getReportWriter("scheduled-for-deletion")
PrintWriter failedDeleting = getReportWriter("failed-deleting")

Map holds = [:]
Map validBibs = [:]

// Turn file with XL id - old hold id - old bib id into a map, keyed by XL id
new File(scriptDir, "id-oldid-bibid.tsv").each {
    def (id, oldId, bibId) = it.split(/\t/).collect { it.trim() }
    holds[id] = ["oldId": oldId, "bibId": bibId]
}

// Make sure the referenced bib records still exist. Fetch them using the legacy IDs
// we got from Voyager, and map them to their XL shortIds.
selectByIds(holds.collect { "http://libris.kb.se/resource/bib/" + it.value.bibId }) { data ->
    if (data.graph[1]["@type"] == "Instance" && data.graph[0].controlNumber)
        validBibs[data.graph[0].controlNumber] = data.doc.shortId
}

selectByIds(holds.keySet() as List) { data ->
    boolean changed = false
    boolean shouldBeDeleted = false
    def instance = data.graph[1]
    String legacyBibId = holds[data.doc.shortId].bibId

    // Broken holds from certain sigels, or ones that link to a nonexistent bib record,
    // should shuffle off this mortal coil
    if (data.doc.sigel in ["Hal", "SEK"]) {
        shouldBeDeleted = true
    } else if (legacyBibId in validBibs) {
        if (!(instance.itemOf instanceof Map))
            instance.itemOf = [:]
        instance.itemOf << ["@id": baseUri.resolve(validBibs[legacyBibId]).toString()]
        changed = true
    } else {
       shouldBeDeleted = true
    }

    if (shouldBeDeleted) {
        scheduledForDeletion.println("${data.doc.getURI()}")
        data.scheduleDelete(onError: { e ->
            failedDeleting.println("Failed to delete ${data.doc.shortId} due to: $e")
        })
        return
    }

    if (changed) {
        scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave(onError: { e ->
            failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
        })
    }
}
