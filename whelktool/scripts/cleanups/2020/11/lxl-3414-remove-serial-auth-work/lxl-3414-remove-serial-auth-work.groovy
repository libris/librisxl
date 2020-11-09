PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter scheduledForDeleting = getReportWriter("scheduled-deletions")
PrintWriter failedUpdating = getReportWriter("failed-updates")
PrintWriter failedDeleting = getReportWriter("failed-deletions")

List ids = new File(scriptDir, 'seriella-auth-verk.txt').readLines()

List fieldsToRemove = ['642', '643', '644', '645', '646']

// Delete those auth works that are not referenced anywhere
selectByIds(ids) { auth ->
    scheduledForDeleting.println("${auth.doc.getURI()}")
    auth.scheduleDelete(onError: { e ->
        failedDeleting.println("${auth.doc.getURI()}")
    })
}

// Remove unhandled from those that could not be deleted
selectByIds(ids) { auth ->
    def unhandled = auth.graph[0]._marcUncompleted

    // unhandled is a list
    assert unhandled instanceof List
    // unhandled contains no other fields than 642-646
    assert unhandled.every { it.size() == 1 && it.keySet()[0] in fieldsToRemove }

    auth.graph[0].remove('_marcUncompleted')

    scheduledForUpdating.println("${auth.doc.getURI()}")
    auth.scheduleSave(onError: { e ->
        failedUpdating.println("Failed to update ${auth.doc.shortId} due to: $e")
    })
}


