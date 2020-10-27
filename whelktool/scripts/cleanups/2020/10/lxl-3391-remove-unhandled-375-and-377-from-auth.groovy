PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String where = "collection = 'auth' and deleted = false and data#>>'{@graph,0,_marcUncompleted}' ~ '\"375\"|\"377\"'"

selectBySqlWhere(where) { auth ->
    // Make '_marcUncompleted' a list
    def marcUncompleted = asList(auth.graph[0]['_marcUncompleted'])

    boolean modified = false

    // Remove all objects with '375' or '377' from '_marcUncompleted'
    marcUncompleted.removeAll { uncompleted ->
        if ("375" in uncompleted || "377" in uncompleted) {
            modified = true
        }
    }

    if (marcUncompleted.isEmpty()) {
        auth.graph[0].remove('_marcUncompleted')
    }

    if (modified) {
        scheduledForUpdating.println("${auth.doc.getURI()}")
        auth.scheduleSave(onError: { e ->
            failedUpdating.println("Failed to update ${auth.doc.shortId} due to: $e")
        })
    }
}

private List asList(Object o) {
    if (o == null)
        return []
    if (o instanceof List)
        return o
    return [o]
}