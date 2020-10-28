PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String mipfesd = 'https://id.kb.se/term/mipfesd%2F5/Economic%20conditions'
String sao = 'https://id.kb.se/term/sao/Ekonomiska%20f%C3%B6rh%C3%A5llanden'

Map newTopic =
        [
            '@type':'Topic',
            'inScheme': [
                    '@id':'https://id.kb.se/term/mipfesd'
            ],
            'prefLabel':'Economic conditions'
        ]

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

    boolean hasSao = false

    for (i = 0; i < subjects.size(); i++) {
        // If mipfesd is referenced directly under subject, save index for later removal
        if (subjects[i]['@id'] == mipfesd) {
            hasMipfesd = true
            mipfesdIdx = i
        }
        // Check if sao term is also referenced, add later if missing
        if (subjects[i]['@id'] == sao) {
            hasSao = true
        }
        // If mipfesd is referenced in ComplexSubject, replace mipfesd with Topic and add new mipfesd as concept scheme
        if (subjects[i]['@type'] == 'ComplexSubject') {
            termComps = subjects[i]['termComponentList']
            for (j = 0; j < termComps.size(); j++) {
                if (termComps[j]['@id'] == mipfesd) {
                    termComps[j] = ['@type':'Topic', 'prefLabel':'Economic conditions']
                    subjects[i]['inScheme'] = ['@id':'https://id.kb.se/term/mipfesd']
                    modified = true
                }
            }
        }
    }

    // Remove mipfesd, add sao if missing and add new Topic with new mipfesd as concept scheme
    if (hasMipfesd) {
        if (hasSao) {
            subjects.remove(mipfesdIdx)
        } else{
            subjects[mipfesdIdx]['@id'] = sao
        }

        subjects.add(newTopic)

        modified = true
    }

    if (modified) {
        scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave(onError: { e ->
            failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
        })
    }
}
