// A bleak imitation (for hold) of 2018/10/remodel-termComponentList.groovy

PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")

subdivisionTypes = [
        'GenreForm' : 'GenreSubdivision',
        'Temporal' : 'TemporalSubdivision',
        'Geographic' : 'GeographicSubdivision'
]

COMPLEX_SUBJECT_TYPE = 'ComplexSubject'

boolean changeToSubdivision(it, subdivisionTypes) {
    boolean changed = false
    it.termComponentList.eachWithIndex { term, i ->
        if (i != 0) {
            if (subdivisionTypes[term['@type']]) {
                term['@type'] = subdivisionTypes[term['@type']]
                changed = true
            }
        }
    }
    return changed
}

selectBySqlWhere('''
        collection = 'hold' AND data::text LIKE '%"termComponentList"%'
        ''') { data ->
    // guard against missing entity
    if (data.graph.size() < 2) {
        return
    }

    def (record, instance) = data.graph
    instance.subject.findAll {
        it['@type'] == COMPLEX_SUBJECT_TYPE
    }.each {
        if (changeToSubdivision(it, subdivisionTypes)) {
            scheduledForUpdating.println("${data.doc.getURI()}")
            data.scheduleSave()
        }
    }
}
