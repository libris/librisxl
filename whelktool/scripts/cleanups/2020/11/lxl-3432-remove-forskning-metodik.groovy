PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String where = "collection = 'bib' AND data#>>'{@graph,1,instanceOf,subject}' SIMILAR TO '%forskning, ?metodik%'"

selectBySqlWhere(where) { data ->
    List subjects = data.graph[1].instanceOf.subject

    boolean modified = false

    subjects.each { sub ->

        if (sub['@type'] != 'ComplexSubject')
            return

        boolean localRemoved = false
        boolean linkedRemoved = false
        
        modified = sub.termComponentList.removeAll {
            if (it['@type'] == 'TopicSubdivision' && it.prefLabel == 'forskning, metodik')
                return localRemoved = true
            if (it['@id'] == 'https://id.kb.se/term/sao/forskning,metodik')
                return linkedRemoved = true
        } ?: modified

        if (localRemoved) {
            sub.remove('prefLabel')
            sub.remove('sameAs')
            sub.termComponentList << ['@type':'TopicSubdivision', 'prefLabel':'forskning']
            sub.termComponentList << ['@type':'TopicSubdivision', 'prefLabel':'metodik']
        }

        if (linkedRemoved) {
            sub.termComponentList << ['@id':'https://id.kb.se/term/sao/forskning']
            sub.termComponentList << ['@id':'https://id.kb.se/term/sao/metodik']
        }
    }

    if (modified) {
        scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave(onError: { e ->
            failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
        })
    }
}

selectByIds(['42gjkm5n2zds30n']) {
    it.scheduleDelete()
}