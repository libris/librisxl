import whelk.Whelk

PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")
PrintWriter linkChanges = getReportWriter("link-changes")

String where = "collection <> 'definitions'"

selectBySqlWhere(where) { data ->
    boolean changed = traverse(data.graph, data.doc.shortId, data.whelk, linkChanges, "root")

    if (changed) {
        scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave(onError: { e ->
            failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
        })
    }
}

boolean traverse(Object node, String idForLogging, Whelk whelk, PrintWriter linkChanges, String branchRelation) {
    boolean changed = false

    if (node instanceof Map) {
        Map map = node

        if (map.size() == 1 &&
                map["@id"] &&
                map["@id"].startsWith(baseUri.toString()) &&
                !map["@id"].endsWith("#it") ) {
            String correctLinkTarget = whelk.storage.getThingId(map["@id"])
            if (correctLinkTarget != null && correctLinkTarget != map["@id"]) {
                linkChanges.println("In ${idForLogging}, changed a ${branchRelation} reference ${map["@id"]} , to ${correctLinkTarget}")
                map["@id"] = correctLinkTarget
                changed = true
            }
        }

        for (String key : map.keySet()) {
            if (key != "generationProcess" && key != "heldBy" && key != "derivedFrom" && key != "describedBy")
                changed |= traverse(map[key], idForLogging, whelk, linkChanges, key)
        }
    }

    if (node instanceof List) {
        List list = node
        for (Object e : list) {
            changed |= traverse(e, idForLogging, whelk, linkChanges, branchRelation)
        }
    }

    return changed
}
