PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")
PrintWriter scheduledForDeleting = getReportWriter("scheduled-deletions")
PrintWriter failedDeleting = getReportWriter("failed-deletions")

String mipfesd = 'https://id.kb.se/term/mipfesd%2F5/Economic%20conditions'

String where = """
       collection = 'bib' 
       AND data#>>'{@graph,1,instanceOf,subject}' 
       LIKE '%https://id.kb.se/term/mipfesd\\%2F5/Economic\\%20conditions%'
       """

selectBySqlWhere(where) { data ->
    List subjects = data.graph[1]['instanceOf']['subject']

    boolean modified = false

    boolean hasMipfesd = false
    int mipfesdIdx

    for (int i = 0; i < subjects.size(); i++) {
        // If mipfesd is referenced directly under subject, save index for later removal
        if (subjects[i]['@id'] == mipfesd) {
            hasMipfesd = true
            mipfesdIdx = i
        }

        // If mipfesd is referenced in ComplexSubject, replace it with Topic
        if (subjects[i]['@type'] == 'ComplexSubject') {
            List termComps = subjects[i]['termComponentList']
            for (int j = 0; j < termComps.size(); j++) {
                if (termComps[j]['@id'] == mipfesd) {
                    termComps[j] = ['@type':'Topic', 'prefLabel':'Economic conditions']
                    modified = true
                }
            }
        }
    }

    // Remove mipfesd reference from subjects
    if (hasMipfesd) {
        subjects.remove(mipfesdIdx)
        modified = true
    }

    if (modified) {
        scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave(onError: { e ->
            failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
        })
    }
}

// Delete the actual mipfesd auth post as no references to it should remain
selectByIds(['jgvxz1t25kg2znj']) { auth ->
    scheduledForDeleting.println("${auth.doc.getURI()}")
    auth.scheduleDelete(onError: { e ->
        failedDeleting.println("Failed to update ${auth.doc.shortId} due to: $e")
    })
}