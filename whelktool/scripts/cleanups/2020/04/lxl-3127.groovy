PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String where = "collection = 'bib' or collection = 'auth'"

selectBySqlWhere(where) { data ->
    boolean changed = false

    for (Map part : data.graph) {
        changed |= checkEntityForTitles(part)

        if (part.isPartOf) {
            if (part.isPartOf instanceof List) {
                for (Object entity : part.isPartOf) {
                    changed |= checkEntityForTitles( (Map) entity )
                }
            } else
                changed |= checkEntityForTitles( (Map) part.isPartOf )
        }
    }

    if (changed) {
        scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave(onError: { e ->
            failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
        })
    }
}

boolean checkEntityForTitles(Map entity) {
    boolean changed = false
    if (entity.hasTitle) {
        if (entity.hasTitle instanceof List) {
            for (Object titleEntity : entity.hasTitle) {
                changed |= cleanTitle( (Map) titleEntity )
            }
        } else
            changed |= cleanTitle( (Map) entity.hasTitle )
    }
    return changed
}

boolean cleanTitle(Map titleEntity) {
    boolean changed = false
    changed |= (titleEntity.remove("marc:searchElement") != null)
    changed |= (titleEntity.remove("marc:searchControl") != null)
    changed |= (titleEntity.remove("marc:isAddedEntry") != null)
    changed |= (titleEntity.remove("marc:showRemark") != null)
    return changed
}