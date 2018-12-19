IMPORT_DATE = new Date().parse('yyyy-MM-dd', '2018-06-07')


boolean setFullLevel (record) {
    createdDate = new Date().parse('yyyy-MM-dd', record.created)

    if (createdDate.before(IMPORT_DATE))
        return true
    else if (record.bibliography?.find { it['sigel']?.contains('NB')})
        return true
    else if (record.technicalNote?.find { it['label']?.contains('Imported from')})
        return true
}

selectByCollection('bib') { data ->
    // guard against missing entity
    if (data.graph.size() < 2) {
        return
    }

    // bib
    if (data.graph[1].containsKey('instanceOf')) {
        def (record, instance, work) = data.graph

        if (!record) return

        // If encodingLevel already exists, don't do anything
        if (record.encodingLevel) {
            return
        }

        if (setFullLevel(record)) {
            record.put('encodingLevel', 'marc:FullLevel')
            data.scheduleSave()
        } else {
            println("${data.graph[0][ID]} Will not update.")
        }
    }
}
